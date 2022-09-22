package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.bean.TradeTrademarkCategoryUserRefundBean;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * TODO 26、交易域品牌-品类-用户粒度退单各窗口汇总表
 * 1）从 Kafka 退单明细主题读取数据
 * 2）过滤 null 数据并转换数据结构
 * 3）按照唯一键去重
 * 4）转换数据结构
 * 5）补充维度信息
 * 6）设置水位线
 * 7）分组、开窗、聚合
 * 8）写出到 ClickHouse
 * Project: gmall-flink-3.0
 * Package: com.atguigu.app.dws
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/9/22 21:58
 */
public class DwsTradeTrademarkCategoryUserRefundWindow {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.1 状态后端设置
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//                3, Time.days(1), Time.minutes(1)
//        ));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(
//                "hdfs://hadoop102:8020/ck"
//        );
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 2.读取DWD层退单主题数据创建流
        String topic = "dwd_trade_order_refund";
        String groupId = "dws_trade_trademark_category_user_refund_window_211027";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));

        //TODO 3.转换为JSON对象，然后过滤去重数据（使用状态编程的方式取第一条数据）
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                if (!"".equals(value)) {
                    out.collect(JSON.parseObject(value));
                }
            }
        });
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.keyBy(json -> json.getString("id"))
                .filter(new RichFilterFunction<JSONObject>() {

                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("value-state", String.class);
                        StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.seconds(5))
                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                                .build();
                        stateDescriptor.enableTimeToLive(ttlConfig);
                        valueState = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public boolean filter(JSONObject value) throws Exception {

                        //取出状态数据
                        String state = valueState.value();

                        if (state == null) {
                            valueState.update("1");
                            return true;
                        } else {
                            return false;
                        }
                    }
                });

        //TODO 4.将每行数据转换为JavaBean对象
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tradeTrademarkDS = filterDS.map(json ->
                TradeTrademarkCategoryUserRefundBean
                        .builder()
                        .skuId(json.getString("sku_id"))
                        .userId(json.getString("user_id"))
                        .refundCount(1L)
                        .ts(DateFormatUtil.toTs(json.getString("create_time"), true))
                        .build());

        //TODO 5.关联维表
        //5.1 SKU
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanWithSkuDS = AsyncDataStream.unorderedWait(
                tradeTrademarkDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getSkuId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                        input.setTrademarkId(dimInfo.getString("TM_ID"));
                    }
                }, 60, TimeUnit.SECONDS);

        //5.2 Trademark
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanWithTradeMarkDS = AsyncDataStream.unorderedWait(
                beanWithSkuDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getTrademarkId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setTrademarkName(dimInfo.getString("TM_NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        //5.3 Category3
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanWithCategory3DS = AsyncDataStream.unorderedWait(
                beanWithTradeMarkDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory3Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setCategory3Name(dimInfo.getString("NAME"));
                        input.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
                    }
                }, 60, TimeUnit.SECONDS);

        //5.4 Category2
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanWithCategory2DS = AsyncDataStream.unorderedWait(
                beanWithCategory3DS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY2") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory2Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setCategory2Name(dimInfo.getString("NAME"));
                        input.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));
                    }
                }, 60, TimeUnit.SECONDS);

        //5.5 Category1
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanWithCategory1DS = AsyncDataStream.unorderedWait(
                beanWithCategory2DS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY1") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory1Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setCategory1Name(dimInfo.getString("NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        //打印测试
        beanWithCategory1DS.print("beanWithCategory1DS>>>>>>>>>");

        //TODO 6.提取时间戳生成WaterMark、分组、开窗聚合
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceDS = beanWithCategory1DS.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public long extractTimestamp(TradeTrademarkCategoryUserRefundBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                }))
                .keyBy(new KeySelector<TradeTrademarkCategoryUserRefundBean, String>() {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean value) throws Exception {
                        return value.getSkuId() + "-" +
                                value.getTrademarkId() + "-" +
                                value.getTrademarkName() + "-" +
                                value.getCategory1Id() + "-" +
                                value.getCategory1Name() + "-" +
                                value.getCategory2Id() + "-" +
                                value.getCategory2Name() + "-" +
                                value.getCategory3Id() + "-" +
                                value.getCategory3Name();
                    }
                }).window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean value1, TradeTrademarkCategoryUserRefundBean value2) throws Exception {
                        value1.setRefundCount(value1.getRefundCount() + value2.getRefundCount());
                        return value1;
                    }
                }, new WindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeTrademarkCategoryUserRefundBean> input, Collector<TradeTrademarkCategoryUserRefundBean> out) throws Exception {

                        //获取数据
                        TradeTrademarkCategoryUserRefundBean refundBean = input.iterator().next();

                        //补充信息
                        refundBean.setTs(System.currentTimeMillis());
                        refundBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        refundBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));

                        //输出数据
                        out.collect(refundBean);
                    }
                });

        //TODO 7.将数据写出到ClickHouse
        reduceDS.print(">>>>>>>>>>>>>>>>>>>");
        reduceDS.addSink(ClickHouseUtil.getJdbcSink("insert into dws_trade_trademark_category_user_refund_window values(?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 8.启动任务
        env.execute("DwsTradeTrademarkCategoryUserRefundWindow");

    }
}

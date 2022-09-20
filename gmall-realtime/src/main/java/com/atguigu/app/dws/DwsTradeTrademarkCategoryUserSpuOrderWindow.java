package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.bean.TradeTrademarkCategoryUserSpuOrderBean;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import com.atguigu.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * 重点需求
 * TODO 24、交易域品牌-品类-用户-SPU粒度下单各窗口汇总表
 * 1、从 Kafka 订单明细主题读取数据。
 * 2、过滤 null 数据并按照唯一键对数据去重。
 * 3、关联维度信息，按照维度分组。
 * 4、统计各维度各窗口的订单数和订单金额。
 * 5、将数据写入 ClickHouse。
 * Project: gmall-flink-3.0
 * Package: com.atguigu.app.dws
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/9/3 11:26
 */
public class DwsTradeTrademarkCategoryUserSpuOrderWindow {
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(4);

        // TODO 2. 状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(
                RestartStrategies.failureRateRestart(
                        3, Time.days(1L), Time.minutes(1L)
                )
        );
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                "hdfs://hadoop102:8020/ck"
        );
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3. 从 Kafka dwd_trade_order_detail 主题读取下单明细数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_trademark_category_user_order_window";
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 4. 过滤 null 数据并转换数据结构
        SingleOutputStreamOperator<String> filteredDS = source.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String jsonStr) throws Exception {
                        if (jsonStr != null) {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            String userId = jsonObj.getString("user_id");
                            String sourceTypeName = jsonObj.getString("source_type_name");
                            if (userId != null && sourceTypeName != null) {
                                return true;
                            }
                        }
                        return false;
                    }
                }
        );
        SingleOutputStreamOperator<JSONObject> mappedStream = filteredDS.map(JSON::parseObject);

        // TODO 5. 按照 order_detail_id 分组
        KeyedStream<JSONObject, String> keyedStream = mappedStream.keyBy(r -> r.getString("id"));

        // TODO 6. 去重
        SingleOutputStreamOperator<JSONObject> processedStream = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<JSONObject> lastValueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastValueState = getRuntimeContext().getState(
                                new ValueStateDescriptor<JSONObject>("last_value_state", JSONObject.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastValue = lastValueState.value();
                        if (lastValue == null) {
                            ctx.timerService().registerProcessingTimeTimer(5000L);
                            lastValueState.update(jsonObj);
                        } else {
                            String lastRowOpTs = lastValue.getString("row_op_ts");
                            String rowOpTs = jsonObj.getString("row_op_ts");
                            if (TimestampLtz3CompareUtil.compare(lastRowOpTs, rowOpTs) <= 0) {
                                lastValueState.update(jsonObj);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws IOException {
                        JSONObject lastValue = lastValueState.value();
                        if (lastValue != null) {
                            out.collect(lastValue);
                        }
                        lastValueState.clear();
                    }
                }
        );

        // TODO 7. 转换数据结构
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> javaBeanStream = processedStream.map(
                jsonObj -> {
                    String orderId = jsonObj.getString("order_id");
                    String userId = jsonObj.getString("user_id");
                    String skuId = jsonObj.getString("sku_id");
                    Double splitTotalAmount = jsonObj.getDouble("split_total_amount");
                    Long ts = jsonObj.getLong("ts");
                    TradeTrademarkCategoryUserSpuOrderBean trademarkCategoryUserOrderBean = TradeTrademarkCategoryUserSpuOrderBean.builder()
                            .orderCount(1L)
                            .userId(userId)
                            .skuId(skuId)
                            .orderAmount(splitTotalAmount)
                            .ts(ts)
                            .build();
                    return trademarkCategoryUserOrderBean;
                }
        );

        // TODO 8. 维度关联  先使用maxwell将mysql中的维表数据倒入到phoenix中
        // 8.1 关联 sku_info 表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withSkuInfoStream = AsyncDataStream.unorderedWait(
                javaBeanStream,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("dim_sku_info".toUpperCase()) {

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setTrademarkId(jsonObj.getString("tm_id".toUpperCase()));
                        javaBean.setCategory3Id(jsonObj.getString("category3_id".toUpperCase()));
                        javaBean.setSpuId(jsonObj.getString("spu_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) {
                        return javaBean.getSkuId();
                    }
                },
                60 * 5, TimeUnit.SECONDS
        );

        // 8.2 关联 spu_info 表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withSpuInfoStream = AsyncDataStream.unorderedWait(
                withSkuInfoStream,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("dim_spu_info".toUpperCase()) {
                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean javaBean, JSONObject dimJsonObj) throws Exception {
                        javaBean.setSpuName(
                                dimJsonObj.getString("spu_name".toUpperCase())
                        );
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) {
                        return javaBean.getSpuId();
                    }
                },
                60 * 5, TimeUnit.SECONDS    //zk的超时时间为60s
        );

        // 8.3 关联品牌表 base_trademark
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withTrademarkStream = AsyncDataStream.unorderedWait(
                withSpuInfoStream,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("dim_base_trademark".toUpperCase()) {
                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setTrademarkName(jsonObj.getString("tm_name".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) {
                        return javaBean.getTrademarkId();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );

        // 8.4 关联三级分类表 base_category3
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withCategory3Stream = AsyncDataStream.unorderedWait(
                withTrademarkStream,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("dim_base_category3".toUpperCase()) {
                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setCategory3Name(jsonObj.getString("name".toUpperCase()));
                        javaBean.setCategory2Id(jsonObj.getString("category2_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) {
                        return javaBean.getCategory3Id();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );

        // 8.5 关联二级分类表 base_category2
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withCategory2Stream = AsyncDataStream.unorderedWait(
                withCategory3Stream,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("dim_base_category2".toUpperCase()) {
                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setCategory2Name(jsonObj.getString("name".toUpperCase()));
                        javaBean.setCategory1Id(jsonObj.getString("category1_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) {
                        return javaBean.getCategory2Id();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );

        // 8.6 关联一级分类表 base_category1
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withCategory1Stream = AsyncDataStream.unorderedWait(
                withCategory2Stream,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("dim_base_category1".toUpperCase()) {
                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setCategory1Name(jsonObj.getString("name".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) {
                        return javaBean.getCategory1Id();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );

        // TODO 9. 设置水位线
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withWatermarkDS = withCategory1Stream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TradeTrademarkCategoryUserSpuOrderBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TradeTrademarkCategoryUserSpuOrderBean>() {

                                    @Override
                                    public long extractTimestamp(TradeTrademarkCategoryUserSpuOrderBean javaBean, long recordTimestamp) {
                                        return javaBean.getTs() * 1000;
                                    }
                                }
                        )
        );

        // TODO 10. 分组
        KeyedStream<TradeTrademarkCategoryUserSpuOrderBean, String> keyedForAggregateStream = withWatermarkDS.keyBy(
                new KeySelector<TradeTrademarkCategoryUserSpuOrderBean, String>() {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) throws Exception {
                        return javaBean.getTrademarkId() +
                                javaBean.getTrademarkName() +
                                javaBean.getCategory1Id() +
                                javaBean.getCategory1Name() +
                                javaBean.getCategory2Id() +
                                javaBean.getCategory2Name() +
                                javaBean.getCategory3Id() +
                                javaBean.getCategory3Name() +
                                javaBean.getUserId() +
                                javaBean.getSpuId() +
                                javaBean.getSpuName();
                    }
                }
        );

        // TODO 11. 开窗
        WindowedStream<TradeTrademarkCategoryUserSpuOrderBean, String, TimeWindow> windowDS = keyedForAggregateStream.window(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        // TODO 12. 聚合
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> reducedStream = windowDS
                .reduce(
                        new ReduceFunction<TradeTrademarkCategoryUserSpuOrderBean>() {
                            @Override
                            public TradeTrademarkCategoryUserSpuOrderBean reduce(TradeTrademarkCategoryUserSpuOrderBean value1, TradeTrademarkCategoryUserSpuOrderBean value2) throws Exception {
                                value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                                value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TradeTrademarkCategoryUserSpuOrderBean, TradeTrademarkCategoryUserSpuOrderBean, String, TimeWindow>() {

                            @Override
                            public void process(String key, Context context, Iterable<TradeTrademarkCategoryUserSpuOrderBean> elements, Collector<TradeTrademarkCategoryUserSpuOrderBean> out) throws Exception {

                                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                                for (TradeTrademarkCategoryUserSpuOrderBean element : elements) {
                                    element.setStt(stt);
                                    element.setEdt(edt);

                                    element.setTs(System.currentTimeMillis());
                                    out.collect(element);
                                }
                            }
                        }
                );

        // TODO 13. 写出到 OLAP 数据库
        SinkFunction<TradeTrademarkCategoryUserSpuOrderBean> jdbcSink =
                ClickHouseUtil.<TradeTrademarkCategoryUserSpuOrderBean>getJdbcSink(
                        "insert into dws_trade_trademark_category_user_spu_order_window values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                );
        reducedStream.<TradeTrademarkCategoryUserSpuOrderBean>addSink(jdbcSink);

        env.execute("");
    }
}

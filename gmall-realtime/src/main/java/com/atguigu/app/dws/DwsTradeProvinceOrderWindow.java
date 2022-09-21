package com.atguigu.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.app.func.OrderDetailFilterFunction;
import com.atguigu.bean.TradeProvinceOrderWindow;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.DateFormatUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * TODO 24、交易域省份粒度下单各窗口汇总表
 * 1）从 Kafka 订单明细主题读取数据
 * 2）过滤 null 数据并转换数据结构
 * 3）按照唯一键去重
 * 4）转换数据结构
 * 5）设置水位线
 * 6）按照省份 ID 和省份名称分组
 * 7）开窗
 * 8）聚合计算
 * 9）关联省份信息
 * 10）写出到 ClickHouse
 * Project: gmall-flink-3.0
 * Package: com.atguigu.app.dws
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/9/21 21:40
 */
public class DwsTradeProvinceOrderWindow {
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

        //TODO 2.读取DWD层order_detail数据并去重过滤
        String groupId = "dws_trade_province_order_window_211027";
        SingleOutputStreamOperator<JSONObject> orderDetailJsonObjDS = OrderDetailFilterFunction.getDwdOrderDetail(env, groupId);

        //TODO 3.将每行数据转换为JavaBean
        SingleOutputStreamOperator<TradeProvinceOrderWindow> tradeProvinceDS = orderDetailJsonObjDS.map(json -> {

            HashSet<String> orderIdSet = new HashSet<>();
            orderIdSet.add(json.getString("order_id"));

            return new TradeProvinceOrderWindow(
                    "",
                    "",
                    json.getString("province_id"),
                    "",
                    orderIdSet,
                    0L,
                    json.getDouble("split_total_amount"),
                    DateFormatUtil.toTs(json.getString("order_create_time"), true));

        });

        //TODO 4.提取时间戳生成WaterMark
        SingleOutputStreamOperator<TradeProvinceOrderWindow> tradeProvinceWithWmDS = tradeProvinceDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeProvinceOrderWindow>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TradeProvinceOrderWindow>() {
            @Override
            public long extractTimestamp(TradeProvinceOrderWindow element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        //TODO 5.分组开窗聚合
        KeyedStream<TradeProvinceOrderWindow, String> keyedStream = tradeProvinceWithWmDS.keyBy(TradeProvinceOrderWindow::getProvinceId);
        WindowedStream<TradeProvinceOrderWindow, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<TradeProvinceOrderWindow> reduceDS = windowedStream.reduce(new ReduceFunction<TradeProvinceOrderWindow>() {
            @Override
            public TradeProvinceOrderWindow reduce(TradeProvinceOrderWindow value1, TradeProvinceOrderWindow value2) throws Exception {
                value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());

                //合并订单ID的集合
                value1.getOrderIdSet().addAll(value2.getOrderIdSet());

                return value1;
            }
        }, new WindowFunction<TradeProvinceOrderWindow, TradeProvinceOrderWindow, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderWindow> input, Collector<TradeProvinceOrderWindow> out) throws Exception {

                //获取数据
                TradeProvinceOrderWindow provinceOrderWindow = input.iterator().next();

                //补充信息
                provinceOrderWindow.setTs(System.currentTimeMillis());
                provinceOrderWindow.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                provinceOrderWindow.setStt(DateFormatUtil.toYmdHms(window.getStart()));

                provinceOrderWindow.setOrderCount((long) provinceOrderWindow.getOrderIdSet().size());

                //输出数据
                out.collect(provinceOrderWindow);
            }
        });

        //TODO 6.关联维表获取省份名称
        SingleOutputStreamOperator<TradeProvinceOrderWindow> tradeProvinceWithProvinceNameDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<TradeProvinceOrderWindow>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(TradeProvinceOrderWindow input) {
                        return input.getProvinceId();
                    }

                    @Override
                    public void join(TradeProvinceOrderWindow input, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            input.setProvinceName(dimInfo.getString("NAME"));
                        }
                    }
                },
                60, TimeUnit.SECONDS);

        //TODO 7.将数据写出到ClickHouse
        tradeProvinceWithProvinceNameDS.print(">>>>>>>>>>>>");
        tradeProvinceWithProvinceNameDS.addSink(ClickHouseUtil.getJdbcSink("insert into dws_trade_province_order_window values(?,?,?,?,?,?,?)"));

        //TODO 8.启动
        env.execute("DwsTradeProvinceOrderWindow");

    }
}

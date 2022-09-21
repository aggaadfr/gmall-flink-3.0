package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TradePaymentWindowBean;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * TODO 25、交易域支付各窗口汇总表
 * 1）从 Kafka 支付成功明细主题读取数据
 * 2）过滤为 null 的数据，转换数据结构
 * 3）按照唯一键分组
 * 4）去重
 * 5）设置水位线，按照 user_id 分组
 * 6）统计独立支付人数和新增支付人数
 * 7）开窗、聚合
 * 8）写出到 ClickHouse
 * Project: gmall-flink-3.0
 * Package: com.atguigu.app.dws
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/9/21 22:05
 */
public class DwsTradePaymentSucWindow {
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

        //TODO 2.读取DWD 成功支付数据主题
        String topic = "dwd_trade_pay_detail_suc";
        String groupId = "dws_trade_payment_suc_window_211027";
        DataStreamSource<String> paymentStrDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));

        //TODO 3.过滤数据并转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = paymentStrDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("value:" + value + "---");
                }
            }
        });

        //TODO 4.按照唯一键分组,支付id
        KeyedStream<JSONObject, String> keyedByDetailIdStream = jsonObjDS.keyBy(json -> json.getString("order_detail_id"));

        //TODO 5.使用状态编程去重,保留第一条数据
        SingleOutputStreamOperator<JSONObject> filterDS = keyedByDetailIdStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("value", String.class);
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.seconds(5))
                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                        .build();
                stateDescriptor.enableTimeToLive(ttlConfig);

                valueState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                String state = valueState.value();
                if (state == null) {
                    valueState.update("1");
                    return true;
                } else {
                    return false;
                }
            }
        });

        //TODO 6.按照Uid分组
        KeyedStream<JSONObject, String> keyedByUserStream = filterDS.keyBy(json -> json.getString("user_id"));

        //TODO 7.提取当日以及总的新增支付人数
        SingleOutputStreamOperator<TradePaymentWindowBean> tradePaymentDS = keyedByUserStream.flatMap(new RichFlatMapFunction<JSONObject, TradePaymentWindowBean>() {

            private ValueState<String> lastPaymentDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastPaymentDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-payment", String.class));
            }

            @Override
            public void flatMap(JSONObject value, Collector<TradePaymentWindowBean> out) throws Exception {

                //取出状态数据
                String lastPaymentDt = lastPaymentDtState.value();

                //取出当前数据日期
                String callbackTime = value.getString("callback_time");
                String curDt = callbackTime.split(" ")[0];

                //定义两个数字
                long paymentSucUniqueUserCount = 0L;
                long paymentSucNewUserCount = 0L;

                if (lastPaymentDt == null) {
                    paymentSucUniqueUserCount = 1L;
                    paymentSucNewUserCount = 1L;
                    lastPaymentDtState.update(curDt);
                } else if (!lastPaymentDt.equals(curDt)) {
                    paymentSucUniqueUserCount = 1L;
                    lastPaymentDtState.update(curDt);
                }

                if (paymentSucUniqueUserCount == 1L) {
                    out.collect(new TradePaymentWindowBean("",
                            "",
                            paymentSucUniqueUserCount,
                            paymentSucNewUserCount,
                            DateFormatUtil.toTs(callbackTime, true)));
                }
            }
        });

        //TODO 8.提取事件时间、开窗、聚合
        SingleOutputStreamOperator<TradePaymentWindowBean> reduceDS = tradePaymentDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TradePaymentWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TradePaymentWindowBean>() {
                    @Override
                    public long extractTimestamp(TradePaymentWindowBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                })).windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TradePaymentWindowBean>() {
                    //TODO 这里使用的是TumblingEventTimeWindows(滚动窗口)直接返回value即可  如果使用SlidingEventTimeWindows(滑动)的窗口，需要new一个新的对象
                    @Override
                    public TradePaymentWindowBean reduce(TradePaymentWindowBean value1, TradePaymentWindowBean value2) throws Exception {
                        value1.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());
                        value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                        // TODO 要区分滚动窗口和滑动窗口
                        return value1;
                    }
                }, new AllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TradePaymentWindowBean> values, Collector<TradePaymentWindowBean> out) throws Exception {
                        //取出数据
                        TradePaymentWindowBean windowBean = values.iterator().next();
                        //补充信息
                        windowBean.setTs(System.currentTimeMillis());
                        windowBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        windowBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        //输出数据
                        out.collect(windowBean);
                    }
                });

        //TODO 9.将数据写出到ClickHouse
        reduceDS.print(">>>>>>>>>>>");
        reduceDS.addSink(ClickHouseUtil.getJdbcSink("insert into dws_trade_payment_suc_window values(?,?,?,?,?)"));

        //TODO 10.启动任务
        env.execute("DwsTradePaymentSucWindow");

    }
}

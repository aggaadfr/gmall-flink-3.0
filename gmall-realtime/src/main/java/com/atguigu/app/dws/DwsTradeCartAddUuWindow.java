package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.CartAddUuBean;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
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
 * TODO 22、交易域加购各窗口汇总表
 * 1、从Kafka加购明细主题读取数据
 * 2、转换数据结构、设置水位线、按照用户 id 分组、过滤独立用户加购记录
 * 3、开窗、聚合
 * 4、将数据写入 ClickHouse
 * Project: gmall-flink-3.0
 * Package: com.atguigu.app.dws
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/8/19 23:24
 */
public class DwsTradeCartAddUuWindow {
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

        //TODO 2.读取Kafka DWD层加购数据主题 创建流
        String topic = "dwd_trade_cart_add";
        String groupId = "dws_trade_cart_add_uu_window_211027";
        DataStreamSource<String> cartAddStringDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));

        //TODO 3.将每行数据转换为JSON格式
        SingleOutputStreamOperator<JSONObject> jsonObjDS = cartAddStringDS.map(JSON::parseObject);

        //TODO 4.提取时间戳生成WaterMark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                //yyyy-MM-dd HH:mm:ss
                String dateTime = element.getString("operate_time");
                if (dateTime == null) {
                    dateTime = element.getString("create_time");
                }
                return DateFormatUtil.toTs(dateTime, true);
            }
        }));

        //TODO 5.按照 user_id 分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWmDS.keyBy(json -> json.getString("user_id"));

        //TODO 6.过滤出独立用户,同时转换数据结构
        SingleOutputStreamOperator<CartAddUuBean> cartAddDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {

            private ValueState<String> lastCartAddDt;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("cart-add", String.class);
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                stateDescriptor.enableTimeToLive(ttlConfig);

                lastCartAddDt = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void flatMap(JSONObject value, Collector<CartAddUuBean> out) throws Exception {

                //提取状态数据
                String lastDt = lastCartAddDt.value();
                //yyyy-MM-dd HH:mm:ss
                String dateTime = value.getString("operate_time");
                if (dateTime == null) {
                    dateTime = value.getString("create_time");
                }
                String curDt = dateTime.split(" ")[0];

                //如果状态数据为null或者与当前日期不是同一天,则保留数据,更新状态
                if (lastDt == null || !lastDt.equals(curDt)) {
                    lastCartAddDt.update(curDt);
                    out.collect(new CartAddUuBean("", "", 1L, 0L));
                }
            }
        });

        //TODO 7.开窗、聚合
        AllWindowedStream<CartAddUuBean, TimeWindow> windowedStream = cartAddDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        SingleOutputStreamOperator<CartAddUuBean> resultDS = windowedStream.reduce(new ReduceFunction<CartAddUuBean>() {
            @Override
            public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                return value1;
            }
        }, new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<CartAddUuBean> values, Collector<CartAddUuBean> out) throws Exception {

                //取出数据
                CartAddUuBean cartAddUuBean = values.iterator().next();

                //补充窗口信息
                cartAddUuBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                cartAddUuBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                //补充版本信息
                cartAddUuBean.setTs(System.currentTimeMillis());

                //输出数据
                out.collect(cartAddUuBean);

            }
        });

        //TODO 8.将数据写出到ClickHouse
        resultDS.print(">>>>>>>>>>>>");
        resultDS.addSink(ClickHouseUtil.getJdbcSink("insert into dws_trade_cart_add_uu_window values(?,?,?,?)"));

        //TODO 9.启动任务
        env.execute("DwsTradeCartAddUuWindow");

    }

}

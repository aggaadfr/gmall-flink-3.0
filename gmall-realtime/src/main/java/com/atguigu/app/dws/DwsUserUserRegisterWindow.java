package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.UserRegisterBean;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * TODO 21、用户域用户注册各窗口汇总表
 * 1、读取 Kafka 用户注册主题数据
 * 2、转换数据结构(String 转换为 JSONObject)
 * 3、设置水位线
 * 4、开窗、聚合
 * 5、写入 ClickHouse
 * Project: gmall-flink-3.0
 * Package: com.atguigu.app.dws
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/8/16 23:03
 */
public class DwsUserUserRegisterWindow {
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

        //TODO 2.读取用户注册主题数据
        String topic = "dwd_user_register";
        String groupId = "dws_user_user_register_window_211027";
        DataStreamSource<String> registerDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));

        //TODO 3.将每行数据转换为JavaBean对象
        SingleOutputStreamOperator<UserRegisterBean> userRegisterDS = registerDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            return new UserRegisterBean("", "", 1L, jsonObject.getLong("ts") * 1000L);
        });

        //TODO 4.提取时间戳生成Watermark
        SingleOutputStreamOperator<UserRegisterBean> userRegisterWithWmDS = userRegisterDS.assignTimestampsAndWatermarks(WatermarkStrategy.<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<UserRegisterBean>() {
            @Override
            public long extractTimestamp(UserRegisterBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        //TODO 5.开窗、聚合
        AllWindowedStream<UserRegisterBean, TimeWindow> windowedStream = userRegisterWithWmDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<UserRegisterBean> resultDS = windowedStream.reduce(new ReduceFunction<UserRegisterBean>() {
            @Override
            public UserRegisterBean reduce(UserRegisterBean value1, UserRegisterBean value2) throws Exception {
                value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
                return value1;
            }
        }, new AllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<UserRegisterBean> values, Collector<UserRegisterBean> out) throws Exception {
                //提取数据
                UserRegisterBean registerBean = values.iterator().next();

                //补充窗口信息
                registerBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                registerBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                //输出数据
                out.collect(registerBean);
            }
        });

        //TODO 6.将数据写出到ClickHouse
        resultDS.print(">>>>>>>>");
        resultDS.addSink(ClickHouseUtil.getJdbcSink("insert into dws_user_user_register_window values(?,?,?,?)"));

        //TODO 7.启动任务
        env.execute("DwsUserUserRegisterWindow");
    }
}

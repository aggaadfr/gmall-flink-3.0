package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TrafficPageViewBean;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * TODO 18 流量域版本-渠道-地区-访客类别粒度页面浏览各窗口汇总表
 * 1、读取3个主题的数据创建三个流。
 * 2、将3个流统一数据格式 JavaBean
 * 3、提取事件事件生成WaterMark
 * 4、分组、开窗、聚合
 * 5、将数据写出到ClinkHouse
 * <p>
 * Project: gmall-flink-3.0
 * Package: com.atguigu.app.dws
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/7/24 23:10
 */
public class DwsTrafficVcChArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//                3, Time.days(1), Time.minutes(1)
//        ));
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
        env.getCheckpointConfig().setAlignmentTimeout(Duration.ofSeconds(10L));

        System.setProperty("HADOOP_USER_NAME", "atguigu");


        // TODO 2.读取3个主题的数据创建三个流
        //读取dwd_traffic_page_log主题
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_vc_ch_ar_isnew_page_view_window";
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> pageLogSource = env.addSource(kafkaConsumer);
        SingleOutputStreamOperator<JSONObject> jsonObjStream = pageLogSource.map(JSON::parseObject);

        //读取dwd_traffic_user_jump_detail主题
        String ujdTopic = "dwd_traffic_user_jump_detail";
        FlinkKafkaConsumer<String> ujdKafkaConsumer = MyKafkaUtil.getKafkaConsumer(ujdTopic, groupId);
        DataStreamSource<String> ujdSource = env.addSource(ujdKafkaConsumer);

        //读取dwd_traffic_unique_visitor_detail主题
        String uvTopic = "dwd_traffic_unique_visitor_detail";
        FlinkKafkaConsumer<String> uvKafkaConsumer = MyKafkaUtil.getKafkaConsumer(uvTopic, groupId);
        DataStreamSource<String> uvSource = env.addSource(uvKafkaConsumer);


        // TODO 3.将3个流统一数据格式 JavaBean
        //dwd_traffic_page_log主题数据转化
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithPvDS = jsonObjStream.map(
                new MapFunction<JSONObject, TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean map(JSONObject jsonObj) throws Exception {
                        JSONObject common = jsonObj.getJSONObject("common");
                        JSONObject page = jsonObj.getJSONObject("page");

                        // 获取 ts
                        Long ts = jsonObj.getLong("ts");

                        // 获取维度信息
                        String vc = common.getString("vc");
                        String ch = common.getString("ch");
                        String ar = common.getString("ar");
                        String isNew = common.getString("is_new");

                        // 获取页面访问时长
                        Long duringTime = page.getLong("during_time");

                        // 定义变量接受其它度量值
                        Long uvCt = 0L;
                        Long svCt = 0L;
                        Long pvCt = 1L;
                        Long ujCt = 0L;

                        // 判断本页面是否开启了一个新的会话
                        String lastPageId = page.getString("last_page_id");
                        if (lastPageId == null) {
                            svCt = 1L;
                        }

                        // 封装为实体类
                        TrafficPageViewBean trafficPageViewBean = new TrafficPageViewBean(
                                "",
                                "",
                                vc,
                                ch,
                                ar,
                                isNew,
                                uvCt,
                                svCt,
                                pvCt,
                                duringTime,
                                ujCt,
                                ts
                        );
                        return trafficPageViewBean;
                    }
                }
        );

        //dwd_traffic_user_jump_detail主题数据转换
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithUjDS = ujdSource.map(jsonStr -> {
            JSONObject jsonObj = JSONObject.parseObject(jsonStr);
            JSONObject common = jsonObj.getJSONObject("common");
            Long ts = jsonObj.getLong("ts") + 10 * 1000L;

            // 获取维度信息
            String vc = common.getString("vc");
            String ch = common.getString("ch");
            String ar = common.getString("ar");
            String isNew = common.getString("is_new");

            // 封装为实体类
            return new TrafficPageViewBean(
                    "",
                    "",
                    vc,
                    ch,
                    ar,
                    isNew,
                    0L,
                    0L,
                    0L,
                    0L,
                    1L,
                    ts
            );
        });

        //dwd_traffic_unique_visitor_detail主题数据转换
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithUvDS = uvSource.map(jsonStr -> {
            JSONObject jsonObj = JSON.parseObject(jsonStr);
            JSONObject common = jsonObj.getJSONObject("common");
            Long ts = jsonObj.getLong("ts");

            // 获取维度信息
            String vc = common.getString("vc");
            String ch = common.getString("ch");
            String ar = common.getString("ar");
            String isNew = common.getString("is_new");

            // 封装为实体类
            return new TrafficPageViewBean(
                    "",
                    "",
                    vc,
                    ch,
                    ar,
                    isNew,
                    1L,
                    0L,
                    0L,
                    0L,
                    0L,
                    ts
            );
        });


        // TODO 4.提取事件事件生成WaterMark
        //union合并三个主题数据
        DataStream<TrafficPageViewBean> pageViewBeanDS = trafficPageViewWithPvDS.union(
                trafficPageViewWithUjDS,
                trafficPageViewWithUvDS);

        //生成watermark
        SingleOutputStreamOperator<TrafficPageViewBean> unionDS = pageViewBeanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(13))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TrafficPageViewBean>() {
                                    @Override
                                    public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long recordTimestamp) {
                                        return trafficPageViewBean.getTs();
                                    }
                                }
                        )
        );


        // TODO 5.分组、开窗、聚合
        // 分组
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> keyedStream = unionDS.keyBy(trafficPageViewBean ->
                        Tuple4.of(
                                trafficPageViewBean.getVc(),
                                trafficPageViewBean.getCh(),
                                trafficPageViewBean.getAr(),
                                trafficPageViewBean.getIsNew()
                        )
                , Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING)
        );

        //开窗
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(
                        Time.seconds(10L)))
                .allowedLateness(Time.seconds(10L));

        //聚合--增量聚合+窗口 (增量聚合，来一条聚合一条，效率高，占用空间小)
        SingleOutputStreamOperator<TrafficPageViewBean> reduceDS = windowedStream.reduce(
                new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                        //数据进行累加
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                        value1.setUjCt(value1.getUjCt() + value2.getUjCt());
                        return value1;
                    }
                },
                new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple4<String, String, String, String> key, Context context, Iterable<TrafficPageViewBean> elements, Collector<TrafficPageViewBean> out) throws Exception {
                        //将前面ReduceFunction聚合后的单一结果进行开窗
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        for (TrafficPageViewBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            //补充窗口信息并输出
                            out.collect(element);
                        }
                    }
                }
        );


        //全量聚合(全量聚合，占用空间大)
//        windowStream.apply(new WindowFunction<TrafficPageViewBean, Object, Tuple4<String, String, String, String>, TimeWindow>() {
//            @Override
//            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<Object> out) throws Exception {
//
//            }
//        })


        // TODO 6.将数据写出到ClinkHouse
        reduceDS.addSink(ClickHouseUtil.<TrafficPageViewBean>getJdbcSink(
                "insert into dws_traffic_vc_ch_ar_is_new_page_view_window values(?,?,?,?,?,?,?,?,?,?,?,?)"
        ));


        // TODO 7.启动任务
        env.execute("DwsTrafficVcChArIsNewPageViewWindow");


    }
}

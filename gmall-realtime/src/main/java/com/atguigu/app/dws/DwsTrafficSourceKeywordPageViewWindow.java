package com.atguigu.app.dws;

import com.atguigu.app.func.KeywordUDTF;
import com.atguigu.bean.KeywordBean;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * TODO 17 流量域来源关键词粒度页面浏览各窗口汇总表
 * <p>
 * Project: gmall-flink-3.0
 * Package: com.atguigu.app.dws
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/7/19 22:51
 */
public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, Time.days(1), Time.minutes(1)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                "hdfs://hadoop102:8020/ck"
        );
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3. 使用DDL方式读取 DWD层页面浏览日志创建表,同时获取事件时间生成Watermark
        tableEnv.executeSql("" +
                "create table page_log( " +
                "    `page` map<string,string>, " +
                "    `ts` bigint, " +
                "    `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +  //将ts时间戳转换成时间字符
                "    WATERMARK FOR rt AS rt - INTERVAL '2' SECOND " +  //使用DDL窗口函数
                ")" + MyKafkaUtil.getKafkaDDL("dwd_traffic_page_log", "Dws_Traffic_Source_Keyword_PageView_Window_211027"));

        // TODO 4. 过滤出搜索数据
        Table keyWordTable = tableEnv.sqlQuery("" +
                "select " +
                "    page['item'] key_word, " +
                "    rt " +
                "from " +
                "    page_log " +
                "where page['item'] is not null " +
                "and page['last_page_id'] = 'search' " +
                "and page['item_type'] = 'keyword'");
        tableEnv.createTemporaryView("key_word_table", keyWordTable);


        // TODO 5. 使用自定义函数分词处理
        // 5.1 注册自定义函数
        tableEnv.createTemporaryFunction("SplitFunction", KeywordUDTF.class);
        // 5.2 处理数据
        Table splitTable = tableEnv.sqlQuery("" +
                "SELECT  " +
                "    word, " +
                "    rt " +
                "FROM key_word_table, LATERAL TABLE(SplitFunction(key_word))");
        tableEnv.createTemporaryView("split_table", splitTable);

        // TODO 6. 分组开窗聚合
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "    'search' source, " +   //来源search，固定值
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +     //保留窗口开始时间
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +       //保留窗口结束时间
                "    word keyword, " +
                "    count(*) keyword_count, " +
                "    UNIX_TIMESTAMP() ts " +   // 系统时间
                "from " +
                "    split_table " +
                "group by TUMBLE(rt, INTERVAL '10' SECOND),word");  //开启滑动窗口

        // TODO 将数据转化为流
        DataStream<KeywordBean> keywordBeanDataStream = tableEnv.toAppendStream(resultTable, KeywordBean.class);
        keywordBeanDataStream.print(">>>>>>>>>>");

        // TODO 7. 将数据写出到ClinkHouse(先在ClinkHouse中创建出表)
        SinkFunction<KeywordBean> jdbcSink = ClickHouseUtil.<KeywordBean>getJdbcSink(
                "insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?,?)");
        keywordBeanDataStream.addSink(jdbcSink);

        // TODO 8. 启动任务
        env.execute("DwsTrafficSourceKeywordPageViewWindow");


    }
}

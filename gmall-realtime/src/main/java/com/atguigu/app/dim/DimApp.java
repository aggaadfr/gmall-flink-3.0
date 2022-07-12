package com.atguigu.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimSinkFunction;
import com.atguigu.app.func.TableProcessFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * TODO 1 数仓分层
 * 1、消费kafka topic_db主题数据（包含所有的业务表数据）
 * 2、过滤维表数据
 * 3、将数据写入phoenix（每张维表对应一张phoenix表）
 * <p>
 * <p>
 * Project: gmall-flink-3.0
 * Package: com.atguigu.app.dim
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/5/7 23:24
 */
@Slf4j
public class DimApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.setStateBackend(new HashMapStateBackend());
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setCheckpointStorage("hdfs:xxx:8020//xxx/xx");

        String topic = "topic_db";
        String groupId = "topic_db_DimApp";

        //1、消费kafka ODS主题数据
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));


        //2、判断是否为json格式。是写入到主流。不是写入侧输出流，并将数据持久化，保存脏数据
        OutputTag<String> dirtyDataTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    //解释失败写入侧输出流
                    context.output(dirtyDataTag, s);
                }
            }
        });

        //获取侧输出流数据，并打印（保存）
        DataStream<String> sideOutput = jsonObjDS.getSideOutput(dirtyDataTag);
        sideOutput.print("dirtyDataTag >>>>>>>> ");

        //3、使用FLinkCDC读取mysql配置表，并创建广播流
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-211027-config")
                .tableList("gmall-211027-config.table_process")
                .deserializer(new JsonDebeziumDeserializationSchema())  //json的序列化和反序列化方式
                .startupOptions(StartupOptions.initial())               //初始化时加载全量数据
                .build();

        DataStreamSource<String> mysqlSourceDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlSource");

        //定义一个map类型的状态
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        //将广播变量数据放入状态中
        BroadcastStream<String> broadcastStream = mysqlSourceDS.broadcast(mapStateDescriptor);

        //4、连接主流和广播流，主流数据根据广播数据做处理
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);
        SingleOutputStreamOperator<JSONObject> hbaseDS = connectedStream.process(new TableProcessFunction(mapStateDescriptor));


        //5、将过滤后的维度数据写入DIM（phoenix）
        hbaseDS.print(">>>>>>>>>>>>>");
        hbaseDS.addSink(new DimSinkFunction());


        env.execute("DimApp");

    }
}

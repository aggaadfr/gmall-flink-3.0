package com.atguigu.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * DIM层合并主流和广播流处理逻辑，并将过滤后的维度数据写入DIM（phoenix）
 * <p>
 * Project: gmall-flink-3.0
 * Package: com.atguigu.app.func
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/5/8 22:22
 */
@Slf4j
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private MapStateDescriptor<String, TableProcess> mapStateDescriptor = null;
    private Connection connection;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 连接phoenix
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    // 主流数据
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        //1.获取广播的配置数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        TableProcess tableProcess = broadcastState.get(jsonObject.getString("table"));

        String type = jsonObject.getString("type");

        //2.根据sinkColumns配置信息过滤字段
        //只需要bootstrap-insert、insert、update类型的数据，delete和bootstrap-start、bootstrap-complete数据需要过滤掉
        if (tableProcess != null && ("bootstrap-insert".equals(type) || "insert".equals(type) || "update".equals(type))) {

            String sinkColumns = tableProcess.getSinkColumns();
            String[] split = sinkColumns.split(",");

            List<String> columnsList = Arrays.asList(split);
            // 流式删除多余信息，只保留 sinkColumns 字段信息
            jsonObject.entrySet().removeIf(str -> !columnsList.contains(str.getKey()));
        }


        //3.补充sinkTable字段写出
        jsonObject.put("sinkTable", tableProcess.getSinkTable());
        collector.collect(jsonObject);
    }

    // 广播流数据
    //Value:{"before":null,"after":{"source_table":1,"sink_table":"三星","sink_columns":"/static/default.jpg"....},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1649744439676,"snapshot":"false","db":"gmall-210927-flink","sequence":null,"table":"base_trademark","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1649744439678,"transaction":null}
    //Mysql表中的字段是source_table，而javaBean中的字段是sourceTable，json会自动自动转化
    @Override
    public void processBroadcastElement(String s, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        //1.获取并解析数据为JavaBean对象
        JSONObject jsonObject = JSON.parseObject(s);

        //得到新增的配置信息，并将该行信息解析成bean对象
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        //2.去校验表是否存在,如果不存在则建表
        String sinkTable = tableProcess.getSinkTable();
        String sinkColumns = tableProcess.getSinkColumns();
        String sinkPk = tableProcess.getSinkPk();
        String sinkExtend = tableProcess.getSinkExtend();

        // 解析配置文件，并转成sql语句
        StringBuilder createTabelSql = new StringBuilder("create table if not exists ")
                .append(GmallConfig.HBASE_SCHEMA + "." + sinkTable)
                .append("(");
        String[] columns = sinkColumns.split(",");

        for (int i = 0; i < columns.length; i++) {

            String column = columns[i];

            //判断是否为主键字段
            if (sinkPk.equals(column)) {
                createTabelSql.append(column).append(" varchar primary key");
            } else {
                createTabelSql.append(column).append(" varchar");
            }

            //不是最后一个字段,则添加","
            if (i < columns.length - 1) {
                createTabelSql.append(",");
            }
        }

        createTabelSql.append(") ").append(sinkExtend);

        PreparedStatement preparedStatement = null;
        try {
            //预编译SQL
            preparedStatement = connection.prepareStatement(createTabelSql.toString());
            //DDL语句不需要commit
            preparedStatement.execute();
        } catch (Exception e) {
            log.error("建表" + sinkTable + "失败！");
            throw new RuntimeException("建表 " + sinkTable + " 失败！！！！");
        } finally {
            //资源释放
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }


        //3.将数据写入状态
        String key = tableProcess.getSourceTable();
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        // sourcetable, 表字段信息
        broadcastState.put(key, tableProcess);
    }
}

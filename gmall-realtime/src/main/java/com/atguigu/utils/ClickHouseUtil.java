package com.atguigu.utils;

import com.atguigu.bean.TransientSink;
import com.atguigu.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * ClickHouse相关工具类
 * Project: gmall-flink-3.0
 * Package: com.atguigu.utils
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/7/19 22:48
 */
public class ClickHouseUtil {
    public static <T> SinkFunction<T> getJdbcSink(String sql) {
        return JdbcSink.<T>sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T obj) throws SQLException {
                        Field[] declaredFields = obj.getClass().getDeclaredFields();
                        int skipNum = 0;
                        for (int i = 0; i < declaredFields.length; i++) {
                            Field declaredField = declaredFields[i];
                            TransientSink transientSink = declaredField.getAnnotation(TransientSink.class);
                            //发射注解，过滤注解字段
                            if (transientSink != null) {
                                skipNum++;
                                continue;
                            }
                            declaredField.setAccessible(true);
                            try {
                                //获取数据并给占位符复值
                                Object value = declaredField.get(obj);
                                preparedStatement.setObject(i + 1 - skipNum, value);
                            } catch (IllegalAccessException e) {
                                System.out.println("ClickHouse 数据插入 SQL 占位符传参异常 ~");
                                e.printStackTrace();
                            }
                        }
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(5000L) //5s执行一次
                        .withBatchSize(5)   //一次写5条数据
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build()
        );
    }

    
}

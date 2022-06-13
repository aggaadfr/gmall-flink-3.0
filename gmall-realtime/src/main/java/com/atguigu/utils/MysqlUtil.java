package com.atguigu.utils;

/**
 * FlinkSQL---Lookup读取mysql工具类
 * <p>
 * Project: gmall-flink-3.0
 * Package: com.atguigu.utils
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/6/12 22:28
 */
public class MysqlUtil {

    /**
     * 获取维表
     *
     * @return
     */
    public static String getBaseDicLookUpDDL() {
        return "create table `base_dic`( " +
                "    `dic_code` string, " +
                "    `dic_name` string, " +
                "    `parent_code` string, " +
                "    `create_time` timestamp, " +
                "    `operate_time` timestamp, " +
                "    primary key(`dic_code`) not enforced " +
                ")" + MysqlUtil.mysqlLookUpTableDDL("base_dic");
    }

    /**
     * FlinkSQL Mysql连接信息
     *
     * @param tableName mysql中表名
     * @return
     */
    private static String mysqlLookUpTableDDL(String tableName) {
        return "WITH ( " +
                "'connector' = 'jdbc', " +
                "'url' = 'jdbc:mysql://hadoop102:3306/gmall-211027-flink', " +
                "'table-name' = '" + tableName + "', " +
                "'lookup.cache.max-rows' = '100', " +
                "'lookup.cache.ttl' = '1 hour', " +
                "'username' = 'root', " +
                "'password' = '000000', " +
                "'driver' = 'com.mysql.cj.jdbc.Driver' " +
                ")";
    }
}

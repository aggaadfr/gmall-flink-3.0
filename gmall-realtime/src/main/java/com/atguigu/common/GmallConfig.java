package com.atguigu.common;

/**
 * 相关配置参数
 * <p>
 * Project: gmall-flink-3.0
 * Package: com.atguigu.common
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/5/8 22:30
 */
public class GmallConfig {
    // Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL211027_REALTIME";

    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    // Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

    // ClickHouse 驱动
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    // ClickHouse 连接 URL
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/gmall_211027";
}

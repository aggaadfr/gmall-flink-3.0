package com.atguigu.app.dwd.db;

import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * TODO 7 交易域下单事务事实表
 * 1、消费Kafka DWD dwd_trade_order_pre_process订单预处理主题
 * 2、过滤出订单明细数据
 * 3、将数据写到kafka dwd_trade_order_detail
 * Project: gmall-flink-3.0
 * Package: com.atguigu.app.dwd.db
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/7/12 20:54
 */
public class DwdTradeOrderDetail {
    public static void main(String[] args) {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 启用状态后端
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        // 取消所属作业时，将保留所有检查点状态。
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.setRestartStrategy(
                RestartStrategies.failureRateRestart(3, Time.days(1L), Time.minutes(3L))
        );
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        // 设置时区
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));

        // TODO 3. 读取 Kafka dwd_trade_order_pre_process 主题数据
        tableEnv.executeSql("" +
                "create table dwd_trade_order_pre_process( " +
                "id string, " +
                "order_id string, " +
                "user_id string, " +
                "order_status string, " +
                "sku_id string, " +
                "sku_name string, " +
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
                "date_id string, " +
                "create_time string, " +
                "operate_date_id string, " +
                "operate_time string, " +
                "source_id string, " +
                "source_type string, " +
                "source_type_name string, " +
                "sku_num string, " +
                "split_original_amount string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_total_amount string, " +
                "`type` string, " +
                "`old` map<string,string>, " +
                "od_ts string, " +
                "oi_ts string, " +
                "row_op_ts timestamp_ltz(3) " +
                ")" + MyKafkaUtil.getKafkaDDL(
                "dwd_trade_order_pre_process", "dwd_trade_order_detail"));

        // TODO 4. 过滤下单数据
        Table filteredTable = tableEnv.sqlQuery("" +
                "select " +
                "id, " +
                "order_id, " +
                "user_id, " +
                "sku_id, " +
                "sku_name, " +
                "province_id, " +
                "activity_id, " +
                "activity_rule_id, " +
                "coupon_id, " +
                "date_id, " +
                "create_time, " +
                "source_id, " +
                "source_type source_type_code, " +
                "source_type_name, " +
                "sku_num, " +
                "split_original_amount, " +
                "split_activity_amount, " +
                "split_coupon_amount, " +
                "split_total_amount, " +
                "od_ts ts, " +
                "row_op_ts " +
                "from dwd_trade_order_pre_process " +
                "where `type`='insert'");
        tableEnv.createTemporaryView("filtered_table", filteredTable);

        // TODO 5. 创建 Kafka 下单明细表
        tableEnv.executeSql("" +
                "create table dwd_trade_order_detail( " +
                "id string, " +
                "order_id string, " +
                "user_id string, " +
                "sku_id string, " +
                "sku_name string, " +
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
                "date_id string, " +
                "create_time string, " +
                "source_id string, " +
                "source_type_code string, " +
                "source_type_name string, " +
                "sku_num string, " +
                "split_original_amount string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_total_amount string, " +
                "ts string, " +
                "row_op_ts timestamp_ltz(3), " +
                "primary key(id) not enforced " +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_order_detail"));

        // TODO 6. 将数据写出到 Kafka
        tableEnv.executeSql("insert into dwd_trade_order_detail select * from filtered_table");
    }
}

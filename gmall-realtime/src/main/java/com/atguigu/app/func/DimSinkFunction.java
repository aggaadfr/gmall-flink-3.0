package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Set;

/**
 * Dim层数据写入到phoenix
 * <p>
 * Project: gmall-flink-3.0
 * Package: com.atguigu.app.func
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/5/10 21:57
 */
@Slf4j
public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;

        try {
            String sinkTable = value.getString("sinkTable");
            JSONObject data = value.getJSONObject("data");

            Set<String> columns = data.keySet();
            Collection<Object> values = data.values();

            String insertSql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                    StringUtils.join(columns, ",") + ") values ('" +
                    StringUtils.join(values, "','") + "')";

            //如果当前为更新数据,则需要删除缓存数据
//                    if ("update".equals(value.getString("type"))) {
//                        DimUtil.delDimInfo(sinkTable.toUpperCase(), data.getString("id"));
//                    }

            preparedStatement = connection.prepareStatement(insertSql);
            preparedStatement.execute();
            //对DML语句生效
            connection.commit();

        } catch (Exception e) {
            log.error("插入数据失败！");
        } finally {
            //释放资源
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

    }
}

package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Phoenix查询工具类
 * Project: gmall-flink-3.0
 * Package: com.atguigu.utils
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/9/3 11:18
 */
public class PhoenixUtil {
    // 定义数据库连接对象
    private static Connection conn;

    /**
     * 初始化 SQL 执行环境
     */
    public static void initializeConnection() {
        try {
            // 1. 注册驱动
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            // 2. 获取连接对象
            conn = DriverManager.getConnection("jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181");
            // 3. 设置 Phoenix SQL 执行使用的 schema（对应 mysql 的 database）
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        } catch (ClassNotFoundException classNotFoundException) {
            System.out.println("注册驱动异常");
            classNotFoundException.printStackTrace();
        } catch (SQLException sqlException) {
            System.out.println("获取连接对象异常");
            sqlException.printStackTrace();
        }
    }

    /**
     * Phoenix 表数据导入方法
     *
     * @param sinkTable 写入数据的 Phoenix 目标表名
     * @param data      待写入的数据
     */
    public static void insertValues(String sinkTable, JSONObject data) {
        // 双重校验锁初始化连接对象
        if (conn == null) {
            synchronized (PhoenixUtil.class) {
                if (conn == null) {
                    initializeConnection();
                }
            }
        }
        // 获取字段名
        Set<String> columns = data.keySet();
        // 获取字段对应的值
        Collection<Object> values = data.values();
        // 拼接字段名
        String columnStr = StringUtils.join(columns, ",");
        // 拼接字段值
        String valueStr = StringUtils.join(values, "','");
        // 拼接插入语句
        String sql = "upsert into " + GmallConfig.HBASE_SCHEMA
                + "." + sinkTable + "(" +
                columnStr + ") values ('" + valueStr + "')";

        // 为数据库操作对象赋默认值
        PreparedStatement preparedSt = null;

        // 执行 SQL
        try {
            System.out.println("插入语句为:" + sql);
            preparedSt = conn.prepareStatement(sql);
            preparedSt.execute();
            conn.commit();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            throw new RuntimeException("数据库操作对象获取或执行异常");
        } finally {
            if (preparedSt != null) {
                try {
                    preparedSt.close();
                } catch (SQLException sqlException) {
                    sqlException.printStackTrace();
                    throw new RuntimeException("数据库操作对象释放异常");
                }
            }
        }
    }

    /**
     * Phoenix 表查询方法
     *
     * @param sql 查询数据的 SQL 语句
     * @param clz 返回的集合元素类型的 class 对象
     * @param <T> 返回的集合元素类型
     * @return 封装为 List<T> 的查询结果
     */
    public static <T> List<T> queryList(String sql, Class<T> clz) {
        // 双重校验锁初始化连接对象
        if (conn == null) {
            synchronized (PhoenixUtil.class) {
                if (conn == null) {
                    initializeConnection();
                }
            }
        }
        List<T> resList = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            if (conn == null) {
                synchronized (PhoenixUtil.class) {
                    if (conn == null) {
                        initializeConnection();
                    }
                }
            }
            //获取数据库操作对象
            ps = conn.prepareStatement(sql);
            //执行SQL语句
            rs = ps.executeQuery();

            /**处理结果集
             +-----+----------+
             | ID  | TM_NAME  |
             +-----+----------+
             | 17  | lzls     |
             | 18  | mm       |

             class TM{id,tm_name}
             */
            ResultSetMetaData metaData = rs.getMetaData();
            while (rs.next()) {
                //通过反射，创建对象，用于封装查询结果
                T obj = clz.newInstance();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    Object columnValue = rs.getObject(i);
                    BeanUtils.setProperty(obj, columnName, columnValue);
                }
                resList.add(obj);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从phoenix数据库中查询数据发送异常了~~");
        } finally {
            //释放资源
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return resList;
    }
}

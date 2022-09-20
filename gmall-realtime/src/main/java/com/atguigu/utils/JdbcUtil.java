package com.atguigu.utils;

/**
 * jdbc工具类
 * Project: gmall-flink-3.0
 * Package: com.atguigu.utils
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/9/4 23:28
 */

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * jdbc工具类，通用Mysql、phionx
 * select count(*) from t;                                  单行单列
 * select count(*) from t group by dept_id;                 多行单列
 * select * from t;                                         单行多列
 * select dept_id,count(*) from t group by dept_id;         多行多列
 */
public class JdbcUtil {

    /**
     * @param connection        连接器
     * @param querySql          sql语句
     * @param clz               返回的数据类型
     * @param underScoreToCamel 要不要把内容转化为驼峰命名
     * @param <T>
     * @return
     * @throws Exception
     */
    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clz, boolean underScoreToCamel) throws Exception {

        //1.创建集合用于存放结果数据
        ArrayList<T> resultList = new ArrayList<>();

        //2.预编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);

        //3.执行查询
        ResultSet resultSet = preparedStatement.executeQuery();
        //得到元数据信息
        ResultSetMetaData metaData = resultSet.getMetaData();
        //获得列数
        int columnCount = metaData.getColumnCount();

        //4.遍历resultSet,给每行数据封装泛型对象并将其加入结果集中
        while (resultSet.next()) {

            //创建泛型对象
//            T t = clz.newInstance();
            T t = clz.getDeclaredConstructor().newInstance();

            //给泛型对象赋值
            for (int i = 1; i < columnCount + 1; i++) {

                String columnName = metaData.getColumnName(i);
                if (underScoreToCamel) {
                    //转化驼峰命名
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }

                //给范型对象赋值
//                BeanUtils.copyProperty(t, columnName, resultSet.getObject(i));    //JSONObject => {}
                BeanUtils.setProperty(t, columnName, resultSet.getObject(i));

            }

            //将对象加入集合
            resultList.add(t);

        }

        preparedStatement.close();
        resultSet.close();

        //返回查询结果
        return resultList;

    }

    public static void main(String[] args) throws Exception {

        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        long start = System.currentTimeMillis();
        List<JSONObject> list = queryList(connection, "select * from GMALL210325_REALTIME.DIM_BASE_TRADEMARK", JSONObject.class, true);
        long end = System.currentTimeMillis();
        List<JSONObject> list1 = queryList(connection, "select * from GMALL210325_REALTIME.DIM_BASE_TRADEMARK", JSONObject.class, true);
        long end2 = System.currentTimeMillis();

        System.out.println(end - start);  //195 211 188
        System.out.println(end2 - end);   //13  15
        System.out.println(list);
        System.out.println(list1);

    }

}

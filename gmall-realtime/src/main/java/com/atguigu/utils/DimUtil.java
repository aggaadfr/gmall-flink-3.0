package com.atguigu.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * DIM层删除phoenix的已存在数据
 * <p>
 * Project: gmall-flink-3.0
 * Package: com.atguigu.utils
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/5/8 23:26
 */
public class DimUtil {

    /**
     * 删除维表数据
     *
     * @param tableName
     * @param id
     */
    public static void delDimInfo(String tableName, String id) {
        Jedis jedis = JedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        jedis.del(redisKey);
        jedis.close();
    }

    public static JSONObject getDimInfo(String tableName, String id) {
        return getDimInfo(tableName, Tuple2.of("ID", id));
    }

    /**
     * 查询维度数据优化：旁路缓存
     * 先从Redis中查询维度数据，如果查询到了，那么直接返回；如果在Redis中没有查询到维度数据，
     * 那么发送请求，到phoenix表中将维度数据查询处理，并将查询出来的维度数据放到Redis中缓存起来
     * Redis:<K,V>
     * key:        dim:维度表名:主键1_主键2
     * Value：     String
     * TTL:        1day
     */
    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... columnNameAndValues) {
        //拼接从Redis中查询维度的key
        StringBuilder redisKey = new StringBuilder("dim:" + tableName.toLowerCase() + ":");
        //拼接查询SQL
        StringBuilder selectSql = new StringBuilder("select * from " + tableName + " where ");
        //在java语句中，将可变长参数封装为数据  所以我们需要对数据进行遍历
        for (int i = 0; i < columnNameAndValues.length; i++) {
            Tuple2<String, String> columnNameAndValue = columnNameAndValues[i];
            String columnName = columnNameAndValue.f0;
            String columnValue = columnNameAndValue.f1;
            selectSql.append(columnName + " = '" + columnValue + "'");
            redisKey.append(columnValue);
            if (i < columnNameAndValues.length - 1) {
                selectSql.append(" and ");
                redisKey.append("_");
            }
        }

        //操作Redis的客户端对象
        Jedis jedis = null;
        //从Redis中查询的维度结果
        String dimJsonStr = null;
        //方法的返回结果
        JSONObject dimJsonObj = null;

        try {
            jedis = JedisUtil.getJedis();
            //从Redis中获取维度数据
            dimJsonStr = jedis.get(redisKey.toString());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("从Redis中查询维度数据发生了异常");
        }

        if (dimJsonStr != null && dimJsonStr.length() > 0) {
            //缓存命中   直接将从Redis中查询的结果转换为json对象
            dimJsonObj = JSON.parseObject(dimJsonStr);
        } else {
            //在Redis中没有命中缓存   发送请求到phoenix表中 进行查询
            System.out.println("从phoenix表中查询维度的sql:" + selectSql);

            //底层还是调用的PhoenixUtil，从phoenix表中进行查询
            List<JSONObject> dimList = PhoenixUtil.queryList(selectSql.toString(), JSONObject.class);

            if (dimList != null && dimList.size() > 0) {
                //如果存在维度数据，那么集合中的元素只会有一条
                dimJsonObj = dimList.get(0);
                //将从phoenix表中查询的数据写到Redis中
                if (jedis != null) {
                    jedis.setex(redisKey.toString(), 3600 * 24, dimJsonObj.toJSONString());
                }
            } else {
                System.out.println("在维度表中没有找到对应的维度数据~~~");
            }
        }

        //释放资源
        if (jedis != null) {
//            System.out.println("---关闭Jedis客户端----");
            jedis.close();
        }
        return dimJsonObj;
    }

    //根据维度查询条件  到维度表中查询维度数据
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String>... columnNameAndValues) {
        //拼接查询SQL
        StringBuilder selectSql = new StringBuilder("select * from " + tableName + " where ");
        //在java语句中，将可变长参数封装为数据  所以我们需要对数据进行遍历
        for (int i = 0; i < columnNameAndValues.length; i++) {
            Tuple2<String, String> columnNameAndValue = columnNameAndValues[i];
            String columnName = columnNameAndValue.f0;
            String columnValue = columnNameAndValue.f1;
            selectSql.append(columnName + " = '" + columnValue + "'");
            if (i < columnNameAndValues.length - 1) {
                selectSql.append(" and ");
            }
        }
        System.out.println("从phoenix表中查询维度的sql:" + selectSql);

        //底层还是调用的PhoenixUtil，从phoenix表中进行查询
        List<JSONObject> dimList = PhoenixUtil.queryList(selectSql.toString(), JSONObject.class);
        JSONObject dimJsonObj = null;
        if (dimList != null && dimList.size() > 0) {
            //如果存在维度数据，那么集合中的元素只会有一条
            dimJsonObj = dimList.get(0);
        } else {
            System.out.println("在维度表中没有找到对应的维度数据~~~");
        }
        return dimJsonObj;
    }

    /**
     * 用户redis清除过期缓存
     *
     * @param tableName
     * @param id
     */
    public static void deleteCached(String tableName, String id) {
        String redisKey = "dim:" + tableName.toLowerCase() + ":" + id;

        Jedis jedis = null;
        try {
            jedis = JedisUtil.getJedis();
            jedis.del(redisKey);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("清除Redis中缓存数据发生了异常~~~");
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }


    public static void main(String[] args) {
        //JSONObject dimInfo = getDimInfoNoCache("dim_base_trademark", Tuple2.of("id", "18"));
        //JSONObject dimInfo = getDimInfo("dim_base_trademark", Tuple2.of("id", "18"));
//        JSONObject dimInfo = getDimInfo("dim_base_trademark", "18");
//        System.out.println(dimInfo);
    }

}

package com.atguigu.utils;

import redis.clients.jedis.Jedis;

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
}

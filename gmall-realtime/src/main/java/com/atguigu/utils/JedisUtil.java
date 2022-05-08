package com.atguigu.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Redis连接数据池
 * <p>
 * Project: gmall-flink-3.0
 * Package: com.atguigu.utils
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/5/8 23:29
 */
public class JedisUtil {
    private static JedisPool jedisPool;

    private static void initJedisPool() {
        System.out.println("----初始化Redis连接池-----");
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(100);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(5);
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setMaxWaitMillis(2000);
        poolConfig.setTestOnBorrow(true);
        jedisPool = new JedisPool(poolConfig, "hadoop102", 6379, 10000);
    }

    public static Jedis getJedis() {
        if (jedisPool == null) {
            initJedisPool();
        }
//        System.out.println("----获取Jedis客户端-----");
        return jedisPool.getResource();
    }
}

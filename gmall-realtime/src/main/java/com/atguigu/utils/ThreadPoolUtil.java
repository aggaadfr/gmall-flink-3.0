package com.atguigu.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 线程池工具类   单列模式
 * Project: gmall-flink-3.0
 * Package: com.atguigu.utils
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/9/3 11:24
 */
public class ThreadPoolUtil {
    private static ThreadPoolExecutor poolExecutor;

    public static ThreadPoolExecutor getInstance() {
        if (poolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (poolExecutor == null) {
                    System.out.println("---创建线程池---");
                    poolExecutor = new ThreadPoolExecutor(
                            4,      //核心池子
                            20,                 //如果连接大于4且工作队列满了才会开新的线程，最大为20
                            60 * 5,
                            TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE));
                }
            }
        }
        return poolExecutor;
    }
}

package com.atguigu.gmallpublisher.service;

import java.util.Map;

/**
 * Project: gmall-flink-3.0
 * Package: com.atguigu.gmallpublisher.service
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/9/25 21:45
 */
public interface UvService {
    //获取按照渠道分组的日活数据
    Map getUvByCh(int date);
}

package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * 模板方法设计模式模板接口
 * Project: gmall-flink-3.0
 * Package: com.atguigu.app.func
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/9/3 11:23
 */
public interface DimJoinFunction<T> {
    void join(T obj, JSONObject dimJsonObj) throws Exception;

    //获取维度主键的方法
    String getKey(T obj);
}
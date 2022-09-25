package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.mapper.UvMapper;
import com.atguigu.gmallpublisher.service.UvService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Project: gmall-flink-3.0
 * Package: com.atguigu.gmallpublisher.service.impl
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/9/25 21:45
 */
@Service
public class UvServiceImpl implements UvService {

    @Autowired
    private UvMapper uvMapper;

    @Override
    public Map getUvByCh(int date) {

        //创建HashMap用于存放结果数据
        HashMap<String, BigInteger> resultMap = new HashMap<>();

        //查询ClickHouse获取数据
        List<Map> mapList = uvMapper.selectUvByCh(date);

        //遍历集合,取出渠道和日活数据放入结果集
        for (Map map : mapList) {
            resultMap.put((String) map.get("ch"), (BigInteger) map.get("uv"));
        }

        //返回结果
        return resultMap;
    }
}

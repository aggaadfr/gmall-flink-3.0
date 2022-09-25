package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.mapper.GmvMapper;
import com.atguigu.gmallpublisher.service.GmvService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Project: gmall-flink-3.0
 * Package: com.atguigu.gmallpublisher.service.impl
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/9/25 21:42
 */
@Service
public class GmvServiceImpl implements GmvService {

    @Autowired
    private GmvMapper gmvMapper;

    @Override
    public Double getGmv(int date) {
        return gmvMapper.selectGmv(date);
    }
}

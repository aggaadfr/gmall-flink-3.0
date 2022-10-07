package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.bean.TrafficKeywords;
import com.atguigu.gmall.publisher.mapper.TrafficKeywordsMapper;
import com.atguigu.gmall.publisher.service.TrafficKeywordsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TrafficKeywordsServiceImpl implements TrafficKeywordsService {

    @Autowired
    TrafficKeywordsMapper trafficKeywordsMapper;

    @Override
    public List<TrafficKeywords> getKeywords(Integer date) {
        return trafficKeywordsMapper.selectKeywords(date);
    }
}

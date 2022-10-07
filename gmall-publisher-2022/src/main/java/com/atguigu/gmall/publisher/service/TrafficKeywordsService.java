package com.atguigu.gmall.publisher.service;

import com.atguigu.gmall.publisher.bean.TrafficKeywords;

import java.util.List;

public interface TrafficKeywordsService {
    List<TrafficKeywords> getKeywords(Integer date);
}

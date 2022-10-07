package com.atguigu.gmall.publisher.service;

import com.atguigu.gmall.publisher.bean.ActivityReduceStats;

import java.util.List;

public interface ActivityReduceService {
    List<ActivityReduceStats> getActivityStats(Integer date);
}

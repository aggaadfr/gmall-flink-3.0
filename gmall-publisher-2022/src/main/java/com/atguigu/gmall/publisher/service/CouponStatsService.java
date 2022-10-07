package com.atguigu.gmall.publisher.service;

import com.atguigu.gmall.publisher.bean.CouponReduceStats;

import java.util.List;

public interface CouponStatsService {
    List<CouponReduceStats> getCouponStats(Integer date);
}

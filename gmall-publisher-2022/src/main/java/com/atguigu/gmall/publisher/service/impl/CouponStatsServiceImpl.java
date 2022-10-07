package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.bean.CouponReduceStats;
import com.atguigu.gmall.publisher.mapper.CouponStatsMapper;
import com.atguigu.gmall.publisher.service.CouponStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CouponStatsServiceImpl implements CouponStatsService {

    @Autowired
    private CouponStatsMapper couponStatsMapper;

    @Override
    public List<CouponReduceStats> getCouponStats(Integer date) {
        return couponStatsMapper.selectCouponStats(date);
    }
}

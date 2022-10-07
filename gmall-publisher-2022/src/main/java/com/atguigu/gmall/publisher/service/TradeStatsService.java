package com.atguigu.gmall.publisher.service;

import com.atguigu.gmall.publisher.bean.TradeProvinceOrderAmount;
import com.atguigu.gmall.publisher.bean.TradeProvinceOrderCt;
import com.atguigu.gmall.publisher.bean.TradeStats;

import java.util.List;

public interface TradeStatsService {
    Double getTotalAmount(Integer date);

    List<TradeStats> getTradeStats(Integer date);

    List<TradeProvinceOrderCt> getTradeProvinceOrderCt(Integer date);

    List<TradeProvinceOrderAmount> getTradeProvinceOrderAmount(Integer date);
}

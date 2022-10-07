package com.atguigu.gmall.publisher.service;

import com.atguigu.gmall.publisher.bean.CategoryCommodityStats;
import com.atguigu.gmall.publisher.bean.SpuCommodityStats;
import com.atguigu.gmall.publisher.bean.TrademarkCommodityStats;
import com.atguigu.gmall.publisher.bean.TrademarkOrderAmountPieGraph;

import java.util.List;

public interface CommodityStatsService {
    List<TrademarkCommodityStats> getTrademarkCommodityStatsService(Integer date);

    List<CategoryCommodityStats> getCategoryStatsService(Integer date);

    List<SpuCommodityStats> getSpuCommodityStats(Integer date);

    List<TrademarkOrderAmountPieGraph> getTmOrderAmtPieGra(Integer date);
}

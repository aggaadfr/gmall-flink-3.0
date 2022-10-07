package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.bean.CategoryCommodityStats;
import com.atguigu.gmall.publisher.bean.SpuCommodityStats;
import com.atguigu.gmall.publisher.bean.TrademarkCommodityStats;
import com.atguigu.gmall.publisher.bean.TrademarkOrderAmountPieGraph;
import com.atguigu.gmall.publisher.mapper.CommodityStatsMapper;
import com.atguigu.gmall.publisher.service.CommodityStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CommodityStatsServiceImpl implements CommodityStatsService {

    @Autowired
    private CommodityStatsMapper commodityStatsMapper;

    @Override
    public List<TrademarkCommodityStats> getTrademarkCommodityStatsService(Integer date) {
        return commodityStatsMapper.selectTrademarkStats(date);
    }

    @Override
    public List<TrademarkOrderAmountPieGraph> getTmOrderAmtPieGra(Integer date) {
        return commodityStatsMapper.selectTmOrderAmtPieGra(date);
    }

    @Override
    public List<CategoryCommodityStats> getCategoryStatsService(Integer date) {
        return commodityStatsMapper.selectCategoryStats(date);
    }

    @Override
    public List<SpuCommodityStats> getSpuCommodityStats(Integer date) {
        return commodityStatsMapper.selectSpuStats(date);
    }
}

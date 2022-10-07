package com.atguigu.gmall.publisher.service;

import com.atguigu.gmall.publisher.bean.TrafficVisitorStatsPerHour;
import com.atguigu.gmall.publisher.bean.TrafficVisitorTypeStats;

import java.util.List;

public interface TrafficVisitorStatsService {
    List<TrafficVisitorTypeStats> getVisitorTypeStats(Integer date);

    List<TrafficVisitorStatsPerHour> getVisitorPerHrStats(Integer date);
}

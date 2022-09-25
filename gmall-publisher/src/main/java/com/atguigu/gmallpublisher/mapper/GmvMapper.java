package com.atguigu.gmallpublisher.mapper;

import org.apache.ibatis.annotations.Select;

/**
 * Project: gmall-flink-3.0
 * Package: com.atguigu.gmallpublisher.mapper
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/9/25 21:40
 */
//该接口的实现类交给了mybatis实现了
public interface GmvMapper {
    //查询ClickHouse,获取GMV总数
    @Select("select sum(order_amount) from dws_trade_province_order_window where toYYYYMMDD(stt)=#{date}")
    Double selectGmv(int date);
}

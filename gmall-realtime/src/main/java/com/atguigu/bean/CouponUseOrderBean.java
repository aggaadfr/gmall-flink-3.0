package com.atguigu.bean;

import lombok.Data;

/**
 * 12、工具域优惠券使用（下单）事务事实表  实体类
 * Project: gmall-flink-3.0
 * Package: com.atguigu.bean
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/7/14 22:23
 */
@Data
public class CouponUseOrderBean {
    // 优惠券领用记录 id
    String id;
    // 优惠券 id
    String coupon_id;
    // 用户 id
    String user_id;
    // 订单 id
    String order_id;
    // 优惠券使用日期（下单）
    String date_id;
    // 优惠券使用时间（下单）
    String using_time;
    // 历史数据
    String old;
    // 时间戳
    String ts;
}

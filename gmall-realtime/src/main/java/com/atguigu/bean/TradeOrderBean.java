package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * Project: gmall-flink-3.0
 * Package: com.atguigu.bean
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/8/25 22:26
 */
@Data
@AllArgsConstructor
@Builder
public class TradeOrderBean {
    // 窗口起始时间
    String stt;
    // 窗口关闭时间
    String edt;
    // 下单独立用户数
    Long orderUniqueUserCount;
    // 下单新用户数
    Long orderNewUserCount;
    // 下单活动减免金额
    Double orderActivityReduceAmount;
    // 下单优惠券减免金额
    Double orderCouponReduceAmount;
    // 下单原始金额
    Double orderOriginalTotalAmount;
    // 时间戳
    Long ts;
}

package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * DwsTradePaymentSucWindow 的实体类
 * Project: gmall-flink-3.0
 * Package: com.atguigu.bean
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/9/21 22:04
 */
@Data
@AllArgsConstructor
public class TradePaymentWindowBean {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    // 支付成功独立用户数
    Long paymentSucUniqueUserCount;
    // 支付成功新用户数
    Long paymentSucNewUserCount;
    // 时间戳
    Long ts;
}

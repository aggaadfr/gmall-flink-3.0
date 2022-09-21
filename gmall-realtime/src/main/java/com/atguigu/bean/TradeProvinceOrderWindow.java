package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.Set;

/**
 * DwsTradeProvinceOrderWindow 实体类
 * Project: gmall-flink-3.0
 * Package: com.atguigu.bean
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/9/21 21:39
 */

@Data
@AllArgsConstructor
@Builder
public class TradeProvinceOrderWindow {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 省份 ID
    String provinceId;
    // 省份名称
    @Builder.Default
    String provinceName = "";
    @TransientSink
    Set<String> orderIdSet;
    // 累计下单次数
    Long orderCount;
    // 累计下单金额
    Double orderAmount;
    // 时间戳
    Long ts;
}

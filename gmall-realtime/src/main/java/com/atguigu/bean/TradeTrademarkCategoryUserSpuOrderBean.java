package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * DwsTradeTrademarkCategoryUserSpuOrderWindow 实体类
 * Project: gmall-flink-3.0
 * Package: com.atguigu.bean
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/9/3 11:25
 */
@Data
@AllArgsConstructor
@Builder
public class TradeTrademarkCategoryUserSpuOrderBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 品牌 ID
    String trademarkId;
    // 品牌名称
    String trademarkName;
    // 一级品类 ID
    String category1Id;
    // 一级品类名称
    String category1Name;
    // 二级品类 ID
    String category2Id;
    // 二级品类名称
    String category2Name;
    // 三级品类 ID
    String category3Id;
    // 三级品类名称
    String category3Name;

    // sku_id
    // 该字段不写出数据
    @TransientSink
    String skuId;

    // 用户 ID
    String userId;
    // spu_id
    String spuId;
    // spu 名称
    String spuName;
    // 下单次数
    @Builder.Default
    Long orderCount = 0L;
    // 下单金额
    @Builder.Default
    Double orderAmount = 0.0D;
    // 时间戳
    Long ts;

    public static void main(String[] args) {
        TradeTrademarkCategoryUserSpuOrderBean build = builder().build();
        System.out.println(build);
    }
}

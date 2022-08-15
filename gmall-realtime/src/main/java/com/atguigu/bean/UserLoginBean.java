package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 用户域用户登陆各窗口汇总表
 * Project: gmall-flink-3.0
 * Package: com.atguigu.bean
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/8/14 22:14
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserLoginBean {
    // 窗口起始时间
    String stt;

    // 窗口终止时间
    String edt;

    // 回流用户数
    Long backCt;

    // 独立用户数
    Long uuCt;

    // 时间戳
    Long ts;
}

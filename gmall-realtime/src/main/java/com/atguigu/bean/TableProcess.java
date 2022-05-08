package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * flinkCDC读取mysql表对应的表字段
 * <p>
 * Project: gmall-flink-3.0
 * Package: com.atguigu.bean
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/5/8 22:15
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcess {
    private String sourceTable;
    private String sinkTable;
    // 写入的所有表字段
    private String sinkColumns;
    // 主键字段
    private String sinkPk;
    private String sinkExtend;

}

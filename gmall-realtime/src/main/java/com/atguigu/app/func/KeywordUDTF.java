package com.atguigu.app.func;

import com.atguigu.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * Flink自定义UDTF切词函数
 * <p>
 * Project: gmall-flink-3.0
 * Package: com.atguigu.app.func
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/7/17 23:25
 */

//定义输出的数据(word为列名 String为列数据类型)
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {

    public void eval(String str) {

        try {
            for (String word : KeywordUtil.analyze(str)) {
                collect(Row.of(word));
            }
        } catch (IOException e) {
            collect(Row.of(str));
        }
    }

}

package com.atguigu.gmall.publisher;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * description:
 * Created by 铁盾 on 2022/4/17
 */
public class Test {
    private static String now() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd");
        String date = dtf.format(LocalDateTime.now());
        return date;
    }

    public static void main(String[] args) {
        System.out.println(now());
    }
}

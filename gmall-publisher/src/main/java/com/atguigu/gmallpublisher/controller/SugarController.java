package com.atguigu.gmallpublisher.controller;

import com.atguigu.gmallpublisher.service.GmvService;
import com.atguigu.gmallpublisher.service.UvService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Project: gmall-flink-3.0
 * Package: com.atguigu.gmallpublisher.controller
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/9/25 21:39
 */
//@Controller
@RestController // = @Controller+@ResponseBody
@RequestMapping("/api/sugar")
public class SugarController {
    @Autowired
    private GmvService gmvService;

    @Autowired
    private UvService uvService;

    @RequestMapping("/test")
    //@ResponseBody
    public String test1() {
        System.out.println("aaaaaaa");
        return "{\"id\":\"1001\",\"name\":\"zhangsan\"}";
    }

    @RequestMapping("/test2")
    public String test2(@RequestParam("nn") String name,
                        @RequestParam(value = "age", defaultValue = "18") int age) {
        System.out.println(name + ":" + age);
        return "success";
    }

    @RequestMapping("/gmv")
    public String getGmv(@RequestParam(value = "date", defaultValue = "0") int date) {

        if (date == 0) {
            date = getToday();
        }

        //查询数据
        Double gmv = gmvService.getGmv(date);

        //拼接并返回结果数据
        return "{ " +
                "  \"status\": 0," +
                "  \"msg\": \"\"," +
                "  \"data\": " + gmv +
                "}";
    }

    @RequestMapping("/ch")
    public String getUvByCh(@RequestParam(value = "date", defaultValue = "0") int date) {

        if (date == 0) {
            date = getToday();
        }

        //获取数据
        Map uvByCh = uvService.getUvByCh(date);
        Set chs = uvByCh.keySet();
        Collection uvs = uvByCh.values();

        //拼接JSON字符串并返回结果
        return "{" +
                "  \"status\": 0," +
                "  \"msg\": \"\"," +
                "  \"data\": {" +
                "    \"categories\": [\"" +
                StringUtils.join(chs, "\",\"") +
                "\"]," +
                "    \"series\": [" +
                "      {" +
                "        \"name\": \"日活\"," +
                "        \"data\": [" +
                StringUtils.join(uvs, ",") +
                "]" +
                "      }" +
                "    ]" +
                "  }" +
                "}";

    }

    private int getToday() {
        long ts = System.currentTimeMillis();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return Integer.parseInt(sdf.format(ts));
    }
}

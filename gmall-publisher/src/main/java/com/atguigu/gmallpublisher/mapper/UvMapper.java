package com.atguigu.gmallpublisher.mapper;

import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

/**
 * Project: gmall-flink-3.0
 * Package: com.atguigu.gmallpublisher.mapper
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/9/25 21:41
 */
public interface UvMapper {
    /**
     * 根据渠道分组,获取当日的日活数据
     *
     * @param date 当天日期
     * @return List{
     * Map[(ch->Appstore),(uv->465),(uj->2),...],
     * Map[(ch->xiaomi),(uv->316),(uj->4),...],
     * ...
     * }
     */
    @Select("select ch,sum(uv_ct) uv,sum(uj_ct) uj from dws_traffic_channel_page_view_window where toYYYYMMDD(stt)=#{date} group by ch")
    List<Map> selectUvByCh(int date);
}

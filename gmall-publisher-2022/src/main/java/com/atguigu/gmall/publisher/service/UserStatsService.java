package com.atguigu.gmall.publisher.service;

import com.atguigu.gmall.publisher.bean.UserChangeCtPerType;
import com.atguigu.gmall.publisher.bean.UserPageCt;
import com.atguigu.gmall.publisher.bean.UserTradeCt;

import java.util.List;

public interface UserStatsService {
    List<UserPageCt> getUvByPage(Integer date);

    List<UserChangeCtPerType> getUserChangeCt(Integer date);

    List<UserTradeCt> getTradeUserCt(Integer date);
}

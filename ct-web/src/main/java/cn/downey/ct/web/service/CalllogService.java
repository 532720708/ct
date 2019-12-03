package cn.downey.ct.web.service;

import cn.downey.ct.web.bean.Calllog;

import java.util.List;

/**
 * （Spring）
 */
public interface CalllogService {
    List<Calllog> queryMonthDatas(String tel, String callTime);
}

package cn.downey.ct.web.dao;

import cn.downey.ct.web.bean.Calllog;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

/**
 * 通话日志数据访问对象（MyBatis）
 */
@Repository
public interface CalllogDao {
    List<Calllog> queryMonthDatas(Map<String, Object> paramMap);
}

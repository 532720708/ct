package cn.downey.ct.common.constant;

import cn.downey.ct.common.bean.api.Val;

/**
 * 名称常量枚举类
 */
public enum Names implements Val {
    NANMESPACE("ct")
    ,TABLE("ct:calllog")
    ,COLUMNFAMILY_CALLER("caller")
    ,COLUMNFAMILY_CALLEE("callee")
    ,COLUMNFAMILY_INFO("info")
    ,TOPIC("ct");

    private String name;

    private Names(String name) {
        this.name = name;
    }

    public void setValue(Object val) {
        this.name = (String)val;
    }

    public String getValue() {
        return name;
    }
}

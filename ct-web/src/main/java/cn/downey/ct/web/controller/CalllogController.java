package cn.downey.ct.web.controller;

import cn.downey.ct.web.bean.Calllog;
import cn.downey.ct.web.service.CalllogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.List;

/**
 * 通话日志控制器对象（SpringMVC）
 */
@Controller
public class CalllogController {


    @Autowired
    private CalllogService calllogService;

    @RequestMapping("/query")
    public String query(){ //prefix(/WEB-INF/jsp/) + viewName(query) + suffix(.jsp)
        return "query"; //返回的字符串就是字符名称
    }

    @RequestMapping("/view")
    public String view(String tel, String calltime, Model model){

        //查询统计结果
        List<Calllog> logs = calllogService.queryMonthDatas(tel, calltime);
        model.addAttribute("calllogs",logs);

        return "view";
    }
}

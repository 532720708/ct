package cn.downey.ct.producer.bean;

import cn.downey.ct.common.bean.api.DataIn;
import cn.downey.ct.common.bean.api.DataOut;
import cn.downey.ct.common.bean.api.Producer;
import cn.downey.ct.common.bean.entity.Data;
import cn.downey.ct.common.util.DateUtil;
import cn.downey.ct.common.util.NumberUtil;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * 本地数据文件的生产者
 */
public class LocalFileProducer implements Producer {

    private DataIn in;
    private DataOut out;
    private volatile boolean flg = true;

    public void SetIn(DataIn in) {
        this.in = in;
    }

    public void SetOut(DataOut out) {
        this.out = out;
    }

    /**
     * 生产数据
     */
    public void produce() {
        //读取通讯录数据
        try{
            List<Contact> contacts = in.read(Contact.class);

            while (flg){

                //从通讯录中随机查找两个电话号码（主叫、被叫）
                int call1Index = new Random().nextInt(contacts.size());
                int call2Index;
                do {
                    call2Index = new Random().nextInt(contacts.size());
                } while (call1Index == call2Index);

                Contact call1 = contacts.get(call1Index);
                Contact call2 = contacts.get(call2Index);

                //生成随机的通话时间
                String startDate = "20190101000000";
                String endDate = "20200101000000";

                long startTime = DateUtil.parse(startDate,"yyyyMMddHHmmss").getTime();
                long endTime = DateUtil.parse(endDate,"yyyyMMddHHmmss").getTime();

                //通话时间
                long callTime = startTime + (long)((endTime - startTime) * Math.random());

                //通话时间字符串
                String callTimeString = DateUtil.formart(new Date(callTime),"yyyyMMddHHmmss");

                //生成随机的通话时长
                String duration = NumberUtil.format(new Random().nextInt(3000),4);

                //生成通话记录
                Calllog log = new Calllog(call1.getTel(),call2.getTel(),callTimeString,duration);

                //将通话记录写入文件
                out.write(log);

                Thread.sleep(500);
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    /**
     * 关闭生产者
     * @throws IOException
     */
    public void close() throws IOException {
        if(in!=null){
            in.close();
        }
        if(out!=null){
            out.close();
        }
    }
}

package cn.downey.ct.producer;

import cn.downey.ct.common.bean.api.Producer;
import cn.downey.ct.producer.bean.LocalFileProducer;
import cn.downey.ct.producer.io.LocalFileDataIn;
import cn.downey.ct.producer.io.LocalFileDataOut;

/**
 * 启动对象
 */
public class Bootstrap {
    public static void main(String[] args) throws Exception {

        if(args.length < 2){
            System.out.println("Error! Required format:" +
                    "java -jar Produce.jar pathIn pathOut");
            System.exit(1);
        }

        //构建生产者对象
        Producer producer = new LocalFileProducer();

//        producer.SetIn(new LocalFileDataIn("G:\\bigdata\\ct\\ct-producer\\src\\main\\resources\\contact.log"));
//        producer.SetOut(new LocalFileDataOut("G:\\bigdata\\ct\\ct-producer\\src\\main\\resources\\call.log"));
        producer.SetIn(new LocalFileDataIn(args[0]));
        producer.SetOut(new LocalFileDataOut(args[1]));


        //生产数据
        producer.produce();

        //关闭生产者
        producer.close();
    }
}

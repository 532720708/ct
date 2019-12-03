package cn.downey.ct.consumer;

import cn.downey.ct.common.bean.api.Consumer;
import cn.downey.ct.consumer.bean.CalllogConsumer;

import java.io.IOException;

/**
 * 启动消费者
 */

//采集：[atguigu@hadoop100 flume]$ bin/flume-ng agent -c conf/ -n a1 -f /opt/module/data/flume-exec-kafka.conf
//生产：[atguigu@hadoop100 data]$ java -jar ct-producer.jar /opt/module/data/contact.log /opt/module/data/call.log
public class Bootstrap {
    public static void main(String[] args) throws IOException {

        //创建消费者
        Consumer consumer = new CalllogConsumer();

        //消费数据
        consumer.consume();

        //关闭资源
        consumer.close();


        //使用Kafka消费者获取flume采集的数据

        //将数据存储到hbase
    }
}

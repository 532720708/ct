package cn.downey.ct.consumer.dao;

import cn.downey.ct.common.bean.entity.BaseDao;
import cn.downey.ct.common.constant.Names;
import cn.downey.ct.common.constant.VauleConstant;
import cn.downey.ct.consumer.bean.Calllog;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 *  HBase数据访问对象
 */
public class HBaseDao extends BaseDao {

    /**
     * 初始化
     */
    public void init() throws Exception{
        start();

        createNamespaceNX(Names.NANMESPACE.getValue());
        createTableXX(Names.TABLE.getValue(),
                "cn.downey.ct.consumer.coprocessor.InsertCalleeCoprocessor",
                VauleConstant.REGION_COUNT,
                Names.COLUMNFAMILY_CALLER.getValue(),
                Names.COLUMNFAMILY_CALLEE.getValue());

        end();
    }

    /**
     * 插入对象
     * @param log
     * @throws Exception
     */
    public void insertData(Calllog log) throws Exception{
        log.setRowKey(genRegionNum(log.getCall1(),log.getCalltime())
                + "_" + log.getCall1() + "_" + log.getCalltime()
                + "_" + log.getCall2() + "_" + log.getDuration());
        putData(log);
    }




    /**
     * 插入数据
     * @param value
     */
    public void insertData(String value) throws Exception {

        //将通话日志保存到HBase表中

        //1、获取通话日志数据
        String[] values = value.split("\t");
        String call1 = values[0];
        String call2 = values[1];
        String calltime = values[2];
        String duration = values[3];

        //2、创建数据对象

        /*  rowkey设计
            1)  长度原则
                  最大值64KB，推荐长度为10-100byte
                  最好是8的倍数，能短则短，rowkey如果太长会影响性能
            2)  唯一性原则
                  rowkey应该具备唯一性
            3)  散列原则
                3-1)    盐值散列：不能使用时间戳直接作为rowkey
                        在rowkey前增加随机数
                3-2)    字符串反转：
                        时间戳、电话号码
                3-3)    计算分区号：HashMap

         */

        //Caller
        //rowkey = regionNum + call1 + time + call2 + duration
        String rowkey = genRegionNum(call1,calltime)
                + "_" + call1 + "_" + calltime + "_" + call2 + "_"+duration + "_1";
        Put put = new Put(Bytes.toBytes(rowkey));
        byte[] family = Bytes.toBytes(Names.COLUMNFAMILY_CALLER.getValue());
        put.addColumn(family,Bytes.toBytes("call1"),Bytes.toBytes(call1));
        put.addColumn(family,Bytes.toBytes("call2"),Bytes.toBytes(call2));
        put.addColumn(family,Bytes.toBytes("calltime"),Bytes.toBytes(calltime));
        put.addColumn(family,Bytes.toBytes("duration"),Bytes.toBytes(duration));
        put.addColumn(family,Bytes.toBytes("flg"),Bytes.toBytes("1"));

        //Callee
        //rowkey = regionNum + call2 +time + call1 + duration
//        String calleeRowkey = genRegionNum(call2,calltime)
//                + "_" + call2 + "_" + calltime + "_" + call1 + "_"+duration + "_0";
//        Put calleePut = new Put(Bytes.toBytes(calleeRowkey));
//        byte[] calleeFamily = Bytes.toBytes(Names.COLUMNFAMILY_CALLEE.getValue());
//        calleePut.addColumn(calleeFamily,Bytes.toBytes("call1"),Bytes.toBytes(call2));
//        calleePut.addColumn(calleeFamily,Bytes.toBytes("call2"),Bytes.toBytes(call1));
//        calleePut.addColumn(calleeFamily,Bytes.toBytes("calltime"),Bytes.toBytes(calltime));
//        calleePut.addColumn(calleeFamily,Bytes.toBytes("duration"),Bytes.toBytes(duration));
//        calleePut.addColumn(calleeFamily,Bytes.toBytes("flg"),Bytes.toBytes("0"));


        //3、保存数据
        List<Put> puts = new ArrayList<Put>();
        puts.add(put);
//        puts.add(calleePut);

        putData(Names.TABLE.getValue(),puts);

    }
}

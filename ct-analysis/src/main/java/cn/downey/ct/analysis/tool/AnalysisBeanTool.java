package cn.downey.ct.analysis.tool;

import cn.downey.ct.analysis.io.MySQLBeanOutputFormat;
import cn.downey.ct.analysis.io.MySQLTextOutputFormat;
import cn.downey.ct.analysis.kv.AnalysisKey;
import cn.downey.ct.analysis.kv.AnalysisValue;
import cn.downey.ct.analysis.mapper.AnalysisBeanMapper;
import cn.downey.ct.analysis.mapper.AnalysisTextMapper;
import cn.downey.ct.analysis.reducer.AnalysisBeanReducer;
import cn.downey.ct.analysis.reducer.AnalysisTextReducer;
import cn.downey.ct.common.constant.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

/**
 * 分析数据的工具类
 */
public class AnalysisBeanTool implements Tool {
    public int run(String[] strings) throws Exception {

        Job job = Job.getInstance();
        job.setJarByClass(AnalysisBeanTool.class);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(Names.COLUMNFAMILY_CALLEE.getValue()));

        //mapper
        TableMapReduceUtil.initTableMapperJob(
                Names.TABLE.getValue(),
                scan,
                AnalysisBeanMapper.class,
                AnalysisKey.class,
                Text.class,
                job
        );

        //reducer
        job.setReducerClass(AnalysisBeanReducer.class);
        job.setOutputKeyClass(AnalysisKey.class);
        job.setOutputValueClass(AnalysisValue.class);

        //outputformat
        job.setOutputFormatClass(MySQLBeanOutputFormat.class);

        boolean flg = job.waitForCompletion(true);
        if ( flg ){
            return JobStatus.State.SUCCEEDED.getValue();
        }else {
            return JobStatus.State.FAILED.getValue();
        }
    }

    public void setConf(Configuration configuration) {

    }

    public Configuration getConf() {
        return null;
    }
}

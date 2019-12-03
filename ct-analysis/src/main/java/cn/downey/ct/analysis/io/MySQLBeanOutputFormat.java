package cn.downey.ct.analysis.io;

import cn.downey.ct.analysis.kv.AnalysisKey;
import cn.downey.ct.analysis.kv.AnalysisValue;
import cn.downey.ct.common.util.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * MySQL数据格式化输出对象
 */
public class MySQLBeanOutputFormat extends OutputFormat<AnalysisKey, AnalysisValue> {

    protected static class MySQLRecordWriter extends RecordWriter<AnalysisKey, AnalysisValue>{

        private Connection connection = null;
        private Jedis jedis = null;

        public MySQLRecordWriter(){

            //获取资源
            connection = JDBCUtil.getConnection();
            jedis = new Jedis("hadoop100",6379);

        }

        /**
         * 输出数据
         * @param key
         * @param value
         * @throws IOException
         * @throws InterruptedException
         */
        public void write(AnalysisKey key, AnalysisValue value) throws IOException, InterruptedException {

            PreparedStatement pstmt = null;
            try {
                String insertSQL = "insert into ct_call (telid,dateid,sumcall,sumduration) values (?,?,?,?)";

                pstmt = connection.prepareStatement(insertSQL);
                pstmt.setInt(1,Integer.parseInt(jedis.hget("ct_user",key.getTel())));
                pstmt.setInt(2,Integer.parseInt(jedis.hget("ct_date",key.getDate())));
                pstmt.setInt(3,Integer.parseInt(value.getSumCall()));
                pstmt.setInt(4,Integer.parseInt(value.getSumDuration()));
                pstmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if( pstmt != null){
                    try {
                        pstmt.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }

        }

        /**
         * 释放资源
         * @param taskAttemptContext
         * @throws IOException
         * @throws InterruptedException
         */
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if(connection != null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public RecordWriter<AnalysisKey, AnalysisValue> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new MySQLRecordWriter();
    }

    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    private FileOutputCommitter committer = null;

    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }

    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if (this.committer == null) {
            Path output = getOutputPath(context);
            this.committer = new FileOutputCommitter(output, context);
        }

        return this.committer;
    }
}

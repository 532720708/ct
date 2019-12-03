package cn.downey.ct.analysis.io;

import cn.downey.ct.common.util.JDBCUtil;
import jdk.nashorn.internal.ir.CallNode;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * MySQL数据格式化输出对象
 */
public class MySQLTextOutputFormat extends OutputFormat<Text,Text> {

    protected static class MySQLRecordWriter extends RecordWriter<Text, Text>{

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
        public void write(Text key, Text value) throws IOException, InterruptedException {

            String[] values = value.toString().split("_");
            String sumCall = values[0];
            String sumDuration = values[1];
            PreparedStatement pstmt = null;
            try {
                String insertSQL = "insert into ct_call (telid,dateid,sumcall,sumduration) values (?,?,?,?)";

                String k = key.toString();
                String[] ks = k.split("_");

                String tel = ks[0];
                String date = ks[1];

                pstmt = connection.prepareStatement(insertSQL);
                pstmt.setInt(1,Integer.parseInt(jedis.hget("ct_user",tel)));
                pstmt.setInt(2,Integer.parseInt(jedis.hget("ct_date",date)));
                pstmt.setInt(3,Integer.parseInt(sumCall));
                pstmt.setInt(4,Integer.parseInt(sumDuration));
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

    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
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

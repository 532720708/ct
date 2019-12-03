package cn.downey.ct.cache;

import cn.downey.ct.common.util.JDBCUtil;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 启动缓存客户端，向redis中增加缓存数据
 */
public class Bootstrap {
    public static void main(String[] args) {


        //读取MySQL数据

        //读取用户，时间数据
        Map<String,Integer> userMap = new HashMap<String, Integer>();
        Map<String,Integer> dateMap = new HashMap<String,Integer>();
        Connection connection = null;


        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            connection = JDBCUtil.getConnection();
            String queryUserSql = "select id,tel from ct_user";
            pstmt = connection.prepareStatement(queryUserSql);
            rs = pstmt.executeQuery();
            while( rs.next()) {
                Integer id = rs.getInt(1);
                String tel = rs.getString(2);
                userMap.put(tel,id);
            }
            rs.close();

            String queryDateSql = "select id,year,month,day from ct_date";
            pstmt = connection.prepareStatement(queryDateSql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                Integer id = rs.getInt(1);
                String year = rs.getString(2);
                String month = rs.getString(3);
                if (month.length()==1) {
                    month = "0" + month;
                }
                String day = rs.getString(4);
                if (day.length()==1) {
                    day = "0" + day;
                }
                dateMap.put(year+month+day,id);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(rs != null){
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(pstmt != null){
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(connection != null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        //向redis中存储数据
        Jedis jedis = new Jedis("hadoop100",6379);
        Iterator<String> keyIte = userMap.keySet().iterator();
        while(keyIte.hasNext()){
            String key = keyIte.next();
            Integer value = userMap.get(key);
            jedis.hset("ct_user",key,"" + value);
        }

        keyIte = dateMap.keySet().iterator();
        while(keyIte.hasNext()){
            String key = keyIte.next();
            Integer value = dateMap.get(key);
            jedis.hset("ct_date",key,"" + value);
        }
    }
}

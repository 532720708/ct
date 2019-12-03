package cn.downey.ct.common.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JDBCUtil {

    private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    private static final String MYSQL_URL = "jdbc:mysql://alicloud:3306/ct?useUnicode=true&characterEncoding=UTF-8&useSSL=false";
    private static final String MYSQL_USERNAME = "yiyi";
    private static final String MYSQL_PASSWORD = "123456";
    public static Connection getConnection(){
        Connection conn = null;
        try{
            Class.forName(MYSQL_DRIVER_CLASS);
            conn = DriverManager.getConnection(MYSQL_URL,MYSQL_USERNAME,MYSQL_PASSWORD);
        }catch (Exception e){
            e.printStackTrace();
        }
        return conn;
    }

}

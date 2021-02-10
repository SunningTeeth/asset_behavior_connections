package org.daijb.huat.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author daijb
 * @date 2021/2/9 12:04
 */
public class DBConnectUtil {

    private static final Logger logger = LoggerFactory.getLogger(DBConnectUtil.class);

    /**
     * 获取database连接
     */
    public static Connection getConnection() throws Exception {
        Connection conn = null;
        try {
            String url = "jdbc:mysql://192.168.3.188:3306/csp?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true";
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url, "root", "Admin@123");
        } catch (Exception e) {
            logger.error("get database connection failed due to ", e);
            conn = null;
        }
        if (conn == null) {
            throw new Exception("sql connection is null");
        }
        //设置手动提交
        conn.setAutoCommit(false);
        return conn;
    }

    /**
     * 提交事物
     */
    public static void commit(Connection conn) {
        if (conn != null) {
            try {
                conn.commit();
            } catch (SQLException e) {
                logger.error("提交事物失败,Connection:" + conn);
                e.printStackTrace();
            } finally {
                close(conn);
            }
        }
    }

    /**
     * 事物回滚
     *
     * @param conn
     */
    public static void rollback(Connection conn) {
        if (conn != null) {
            try {
                conn.rollback();
            } catch (SQLException e) {
                logger.error("事物回滚失败,Connection:" + conn);
                e.printStackTrace();
            } finally {
                close(conn);
            }
        }
    }

    /**
     * 关闭连接
     *
     * @param conn
     */
    public static void close(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error("关闭连接失败,Connection:" + conn);
            }
        }
    }
}

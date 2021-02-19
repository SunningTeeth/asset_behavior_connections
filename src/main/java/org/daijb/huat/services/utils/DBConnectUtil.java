package org.daijb.huat.services.utils;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author daijb
 * @date 2021/2/9 12:04
 */
public class DBConnectUtil {

    private static final Logger logger = LoggerFactory.getLogger(DBConnectUtil.class);

    private static volatile Connection connection = null;

    /**
     * 获取database连接
     */
    private static Connection getConn() throws Exception {
        try {
            //创建连接池对象
            ComboPooledDataSource comboPooledDataSource = new ComboPooledDataSource();
            //设置数据源
            comboPooledDataSource.setDriverClass("com.mysql.jdbc.Driver");
            String addr = SystemUtil.getHostIp();
            String username = SystemUtil.getMysqlUser();
            String password = SystemUtil.getMysqlPassword();
            comboPooledDataSource.setJdbcUrl("jdbc:mysql://" + addr + ":3306/csp?useEncoding=true&characterEncoding=utf-8&serverTimezone=UTC");
            comboPooledDataSource.setUser(username);
            comboPooledDataSource.setPassword(password);
            //获取连接
            connection = comboPooledDataSource.getConnection();
        } catch (Exception e) {
            logger.error("get database connection failed due to ", e);
            connection = null;
        }
        if (connection == null) {
            throw new Exception("sql connection is null");
        }
        //设置手动提交
        connection.setAutoCommit(false);
        return connection;
    }

    public static Connection getConnection() throws Exception {
        if (connection == null) {
            synchronized (DBConnectUtil.class) {
                if (connection == null) {
                    connection = getConn();
                }
            }
        }
        return connection;
    }

    /**
     * 提交事物
     */
    public static void commit(Connection conn) {
        if (conn != null) {
            try {
                conn.commit();
            } catch (SQLException e) {
                logger.error("提交事物失败,Connection:" + conn, e);
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
                logger.error("事物回滚失败,Connection:" + conn, e);
                close(conn);
            }
        }
    }

    /**
     * 关闭连接
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

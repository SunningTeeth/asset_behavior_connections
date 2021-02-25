package org.daijb.huat.services.utils;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.plaf.TableHeaderUI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author daijb
 * @date 2021/2/9 12:04
 */
public class DBConnectUtil {

    public static void main(String[] args) {
        Integer a = 120;


        System.out.println(a.equals(null));
    }

    private static final Logger logger = LoggerFactory.getLogger(DBConnectUtil.class);

    /**
     * 创建连接池对象
     */
    private static ComboPooledDataSource dataSource = null;

    static {
        configurer();
    }

    /**
     * 配置连接池
     */
    private static void configurer() {
        try {
            //设置数据源
            dataSource = new ComboPooledDataSource();
            dataSource.setDriverClass("com.mysql.jdbc.Driver");
            String addr = SystemUtil.getHostIp();
            String username = SystemUtil.getMysqlUser();
            String password = SystemUtil.getMysqlPassword();
            dataSource.setJdbcUrl("jdbc:mysql://" + addr + ":3306/csp?useEncoding=true&characterEncoding=utf-8&serverTimezone=UTC");
            dataSource.setUser(username);
            dataSource.setPassword(password);
        } catch (Exception e) {
            logger.error("get database connection failed due to ", e);
        }
    }

    public static Connection getConnection() {
        Connection conn = null;
        if (dataSource == null) {
            configurer();
        }
        try {
            conn = dataSource.getConnection();
        } catch (Throwable throwable) {
            logger.error("get database connection failed ", throwable);
        }
        return conn;
    }

    public static void execUpdateTask(String sql, String... params) {
        Connection connection = getConnection();
        try {
            PreparedStatement ps = connection.prepareStatement(sql);
            for (int i = 1; i <= params.length; i++) {
                ps.setString(i, params[i - 1]);
            }
            ps.execute();
        } catch (Throwable throwable) {
            logger.error("execute update task failed ", throwable);
        }

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

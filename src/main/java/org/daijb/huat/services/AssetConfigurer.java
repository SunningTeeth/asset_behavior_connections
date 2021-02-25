package org.daijb.huat.services;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.daijb.huat.services.utils.SystemUtil;

/**
 * @author daijb
 * @date 2021/2/22 16:59
 * asset注册到flink
 */
public class AssetConfigurer {

    /**
     * 返回建模参数
     */
    public static JDBCInputFormat getAssetJdbcInputFormat() {
        return jdbcInputFormat;
    }

    private static final JDBCInputFormat jdbcInputFormat = getAssetJdbcInputFormat0();

    /**
     * 获取建模参数
     */
    private static JDBCInputFormat getAssetJdbcInputFormat0() {

        //定义field 类型
        TypeInformation<?>[] fieldTypes = new TypeInformation<?>[]{
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO
        };

        //2.定义field name
        String[] fieldNames = new String[]{"entityId", "entityName", "assetIp", "areaId"};

        //3.定义Row类型
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);
        String addr = SystemUtil.getHostIp();
        return JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://" + addr + ":3306/csp?useEncoding=true&characterEncoding=utf-8&serverTimezone=UTC")
                .setUsername(SystemUtil.getMysqlUser())
                .setPassword(SystemUtil.getMysqlPassword())
                .setQuery("select entity_id,entity_name,asset_ip,area_id from asset")
                .setRowTypeInfo(rowTypeInfo)
                .setFetchSize(5000)
                .setAutoCommit(true)
                .finish();
    }
}

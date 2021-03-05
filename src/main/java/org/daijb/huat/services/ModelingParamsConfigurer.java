package org.daijb.huat.services;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.daijb.huat.utils.SystemUtil;

/**
 * @author daijb
 * @date 2021/2/22 16:34
 * @see org.daijb.huat.config.ModelParamsConfigurer
 * @deprecated
 */
public class ModelingParamsConfigurer {

    /**
     * 返回建模参数
     */
    public static JDBCInputFormat getModelingParamsStream() {
        return jdbcInputFormat;
    }

    private static final JDBCInputFormat jdbcInputFormat = getModelingParamsStream0();

    /**
     * 获取建模参数
     */
    private static JDBCInputFormat getModelingParamsStream0() {
        TypeInformation<?>[] fieldTypes = new TypeInformation<?>[]{
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.FLOAT_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.DATE_TYPE_INFO,
        };
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);
        String addr = SystemUtil.getHostIp();
        return JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://" + addr + ":3306/csp?useEncoding=true&characterEncoding=utf-8&serverTimezone=UTC")
                .setUsername(SystemUtil.getMysqlUser())
                .setPassword(SystemUtil.getMysqlPassword())
                .setQuery("select * from modeling_params where model_type='1' and model_child_type='1';")
                .setRowTypeInfo(rowTypeInfo)
                .finish();
    }
}

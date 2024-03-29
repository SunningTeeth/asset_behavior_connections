package org.daijb.huat.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.daijb.huat.AssetBehaviorConstants;
import org.daijb.huat.utils.DbConnectUtil;
import org.daijb.huat.utils.SystemUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * @author daijb
 * @date 2021/3/2 13:59
 * <p>
 * 1.建模参数构建
 * 2.建模参数维护
 * </p>
 */
public class ModelParamsConfigurer implements AssetBehaviorConstants {

    private static final Logger logger = LoggerFactory.getLogger(ModelParamsConfigurer.class);

    private static final JDBCInputFormat jdbcInputFormat = getModelingParamsStream0();

    private static Map<String, Object> modelingParams;

    /**
     * 返回建模参数
     *
     * @return 建模参数k-v
     */
    public static Map<String, Object> getModelingParams() {
        if (modelingParams == null) {
            modelingParams = buildModelingParams();
        }
        return modelingParams;
    }


    /**
     * 从数据库获取建模参数
     */
    public static Map<String, Object> buildModelingParams() {
        Connection connection = DbConnectUtil.getConnection();
        Map<String, Object> result = new HashMap<>(15 * 3 / 4);
        try {
            String sql = "select * from modeling_params" +
                    " where model_type=1 and model_child_type =1" +
                    " and model_switch = 1 and model_switch_2 =1" +
                    " and modify_time < DATE_SUB( NOW(), INTERVAL 10 MINUTE );";
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            while (resultSet.next()) {
                result.put(MODEL_ID, resultSet.getString("id"));
                result.put(MODEL_TYPE, resultSet.getString("model_type"));
                result.put(MODEL_CHILD_TYPE, resultSet.getString("model_child_type"));
                result.put(MODEL_RATE_TIME_UNIT, resultSet.getString("model_rate_timeunit"));
                result.put(MODEL_RATE_TIME_UNIT_NUM, resultSet.getString("model_rate_timeunit_num"));
                result.put(MODEL_RESULT_SPAN, resultSet.getString("model_result_span"));
                result.put(MODEL_RESULT_TEMPLATE, resultSet.getString("model_result_template"));
                result.put(MODEL_CONFIDENCE_INTERVAL, resultSet.getString("model_confidence_interval"));
                result.put(MODEL_HISTORY_DATA_SPAN, resultSet.getString("model_history_data_span"));
                result.put(MODEL_UPDATE, resultSet.getString("model_update"));
                result.put(MODEL_SWITCH, resultSet.getString("model_switch"));
                result.put(MODEL_SWITCH_2, resultSet.getString("model_switch_2"));
                result.put(MODEL_ATTRS, resultSet.getString("model_alt_params"));
                result.put(MODEL_TASK_STATUS, resultSet.getString("model_task_status"));
                result.put(MODEL_MODIFY_TIME, resultSet.getString("modify_time"));
            }
        } catch (Throwable throwable) {
            logger.error("Get modeling parameters from the database error ", throwable);
        }
        logger.info("Get modeling parameters from the database : " + result.toString());
        return result;
    }

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

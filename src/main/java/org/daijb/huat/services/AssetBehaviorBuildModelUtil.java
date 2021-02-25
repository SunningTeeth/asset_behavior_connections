package org.daijb.huat.services;

import org.daijb.huat.services.utils.ConversionUtil;
import org.daijb.huat.services.utils.DBConnectUtil;
import org.daijb.huat.services.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author daijb
 * @date 2021/2/18 17:39
 */
public class AssetBehaviorBuildModelUtil implements AssetBehaviorConstants {

    private static final Logger logger = LoggerFactory.getLogger(AssetBehaviorBuildModelUtil.class);

    private static Map<String, Object> modelingParams;

    private static volatile ModelCycle modelCycle = ModelCycle.HOURS;

    static {
        try {
            modelingParams = buildModelingParams();
            calculateSegmentKey();
        } catch (Exception e) {
            modelingParams = null;
        }
    }

    public static Map<String, Object> getModelingParams() {
        return modelingParams;
    }

    public static ModelCycle getModelCycle() {
        return modelCycle;
    }

    /**
     * 建模频率
     */
    public static String getBuildModelRate() {
        return "" + modelingParams.get(MODEL_RATE_TIME_UNIT_NUM) + modelingParams.get(MODEL_RATE_TIME_UNIT);
    }

    /**
     * 计算目标网段key
     * 需要调用方当 key <= 0 时，停止程序
     */
    public static synchronized int calculateSegmentKey() throws Exception {
        String rate = ConversionUtil.toString(modelingParams.get(MODEL_RATE_TIME_UNIT_NUM));
        String rateUnit = ConversionUtil.toString(modelingParams.get(MODEL_RATE_TIME_UNIT));
        // 建模周期
        int cycle = ConversionUtil.toInteger(modelingParams.get(MODEL_RESULT_SPAN));
        //String key = persistence(null);
        String key = null;

        switch (cycle) {
            case 1: {
                modelCycle = ModelCycle.DAYS;
                //天,频率可以是 ss mm hh
                if (StringUtil.equals(rateUnit, "hh")) {
                    if (StringUtil.isEmpty(key)) {
                        key = rateUnit + "_" + 1;
                    } else {
                        int cursor = key.indexOf("_") + 1;
                        key = rateUnit + "_" + (ConversionUtil.toInteger(key.substring(cursor)) + 1);
                    }
                } else {
                    throw new Exception("cycle : " + cycle + ",rate : " + rate + " is not match");
                }
                break;
            }
            case 2: {
                //周,频率只能是 dd
                modelCycle = ModelCycle.WEEK;
                if (StringUtil.equals(rateUnit, "dd")) {
                    if (StringUtil.isEmpty(key)) {
                        key = rateUnit + "_" + 1;
                    } else {
                        int cursor = key.indexOf("_") + 1;
                        key = rateUnit + "_" + (ConversionUtil.toInteger(key.substring(cursor)) + 1);
                    }
                } else {
                    throw new Exception("cycle : " + cycle + ",rate : " + rate + " is not match");
                }
                break;
            }
            case 3: {
                //季度,频率只能是月
                modelCycle = ModelCycle.QUARTER;
                if (StringUtil.equals(rate, "MM")) {
                    if (StringUtil.isEmpty(key)) {
                        key = rateUnit + "_" + 1;
                    } else {
                        int cursor = key.indexOf("_") + 1;
                        key = rateUnit + "_" + (ConversionUtil.toInteger(key.substring(cursor)) + 1);
                    }
                } else {
                    throw new Exception("cycle : " + cycle + ",rate : " + rate + " is not match");
                }
                break;
            }
            case 4: {
                // 年,频率只能是月
                modelCycle = ModelCycle.YEAR;
                if (StringUtil.equals(rate, "MM")) {
                    if (StringUtil.isEmpty(key)) {
                        key = rateUnit + "_" + 1;
                    } else {
                        int cursor = key.indexOf("_") + 1;
                        key = rateUnit + "_" + (ConversionUtil.toInteger(key.substring(cursor)) + 1);
                    }
                } else {
                    throw new Exception("cycle : " + cycle + ",rate : " + rate + " is not match");
                }
            }
            default: {
                throw new Exception("cycle : " + cycle + ",rate : " + rate + " is not match");
            }
        }
        // persistence(key);
        assert key != null;
        return ConversionUtil.toInteger(key.substring(key.indexOf("_") + 1));
    }

    private static synchronized String persistence(String key) {
        //File file = new File(PERSISTENCE_FILEPATH);
        File file = new File("C:\\daijb\\demop\\");
        if (!file.exists()) {
            file.mkdirs();
        }
        if (StringUtil.isEmpty(key)) {
            File[] childrenFiles = file.listFiles();
            if (childrenFiles.length <= 0) {
                return null;
            }
            return childrenFiles[0].getName();
        } else {
            // 更新
            //File dest = new File(PERSISTENCE_FILEPATH + key);
            File dest = new File("C:\\daijb\\demop\\" + key);
            if (!dest.exists()) {
                try {
                    File[] childFiles = dest.getParentFile().listFiles();
                    if (childFiles != null && childFiles.length > 0) {
                        childFiles[0].delete();
                    }
                    if (dest.createNewFile()) {
                        if (Objects.requireNonNull(file.listFiles())[0].renameTo(dest)) {
                            return dest.getName();
                        }
                    }
                } catch (IOException ignored) {
                }
            }
        }
        return null;
    }

    public static Map<String, Object> buildModelingParams() {
        Connection connection = DBConnectUtil.getConnection();
        Map<String, Object> result = new HashMap<>(15 * 3 / 4);
        try {
            ResultSet resultSet = connection.createStatement().executeQuery("select * from modeling_params where model_type=1 and model_child_type =3 and model_switch = 1 and model_switch_2 =1 and modify_time < DATE_SUB( NOW(), INTERVAL 10 MINUTE );");
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
            logger.error("build model params failed ", throwable);
        }

        return result;
    }


}

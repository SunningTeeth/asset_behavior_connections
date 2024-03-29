package org.daijb.huat.services;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.daijb.huat.utils.DbConnectUtil;

import java.sql.*;
import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * @author daijb
 * @date 2021/2/22 19:03
 */
public class AssetRichSourceConfigurer extends RichSourceFunction<Tuple4<String, String, String, Integer>> {
    private static final long serialVersionUID = 3334654984018091675L;

    private PreparedStatement ps;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = DbConnectUtil.getConnection();
        String sql = "SELECT entity_id, entity_name, asset_ip, area_id FROM asset a, modeling_params m WHERE m.model_alt_params -> '$.model_entity_group' LIKE CONCAT('%', a.entity_groups,'%');";
        ps = connection.prepareStatement(sql);
    }

    @Override
    public void run(SourceContext<Tuple4<String, String, String, Integer>> collect) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            Tuple4<String, String, String, Integer> tuple = new Tuple4<>();
            tuple.setFields(resultSet.getString(1),
                    resultSet.getString(2),
                    resultSet.getString(3),
                    resultSet.getInt(4)
            );
            collect.collect(tuple);
        }

    }

    @Override
    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception ignored) {
        }
    }

}

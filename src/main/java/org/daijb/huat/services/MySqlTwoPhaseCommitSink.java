package org.daijb.huat.services;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;


/**
 * @author daijb
 * @date 2021/2/9 12:09
 * <p>
 * https://blog.51cto.com/simplelife/2401411
 * 这里简单说下这个类的作用就是实现这个类的方法：beginTransaction、preCommit、commit、abort，达到事件（preCommit）预提交的逻辑（当事件进行自己的逻辑处理后进行预提交，如果预提交成功之后才进行真正的（commit）提交，如果预提交失败则调用abort方法进行事件的回滚操作），结合flink的checkpoint机制，来保存topic中partition的offset。
 * <p>
 * 达到的效果我举个例子来说明下：比如checkpoint每10s进行一次，此时用FlinkKafkaConsumer011实时消费kafka中的消息，消费并处理完消息后，进行一次预提交数据库的操作，如果预提交没有问题，10s后进行真正的插入数据库操作，如果插入成功，进行一次checkpoint，flink会自动记录消费的offset，可以将checkpoint保存的数据放到hdfs中，如果预提交出错，比如在5s的时候出错了，此时Flink程序就会进入不断的重启中，重启的策略可以在配置中设置，当然下一次的checkpoint也不会做了，checkpoint记录的还是上一次成功消费的offset，本次消费的数据因为在checkpoint期间，消费成功，但是预提交过程中失败了，注意此时数据并没有真正的执行插入操作，因为预提交（preCommit）失败，提交（commit）过程也不会发生了。等你将异常数据处理完成之后，再重新启动这个Flink程序，它会自动从上一次成功的checkpoint中继续消费数据，以此来达到Kafka到Mysql的Exactly-Once。
 * <p>
 * 作者：it_zzy
 * 链接：https://www.jianshu.com/p/5bdd9a0d7d02
 * 来源：简书
 * 著作权归作者所有。商业转载请联系作者获得授权，非商业转载请注明出处。
 */
public class MySqlTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction<ObjectNode, Connection, Void> {

    private static final Logger logger = LoggerFactory.getLogger(MySqlTwoPhaseCommitSink.class);

    public MySqlTwoPhaseCommitSink() {
        super(new KryoSerializer<>(Connection.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    /**
     * 执行数据库入库操作  task初始化的时候调用
     *
     * @param connection
     * @param objectNode
     * @param context
     * @throws Exception
     */
    @Override
    protected void invoke(Connection connection, ObjectNode objectNode, Context context) throws Exception {
        logger.info("start invoke...");
        String valueStr = objectNode.get("value").toString();
        JSONObject json = (JSONObject) JSONValue.parse(valueStr);
        String value_str = (String) json.get("value");
        String sql = "insert into `mysqlExactlyOnce_test` (`value`,`insert_time`) values (?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, value_str);
        Timestamp value_time = new Timestamp(System.currentTimeMillis());
        ps.setTimestamp(2, value_time);
        logger.info("要插入的数据:{}--{}", value_str, value_time);
        //执行insert语句
        ps.execute();
        //手动制造异常
        if (Integer.parseInt(value_str) == 15) {
            System.out.println(1 / 0);
        }
    }

    /**
     * 获取连接,开启手动提交事物
     */
    @Override
    protected Connection beginTransaction() throws Exception {
        logger.info("start beginTransaction.......");
        return DBConnectUtil.getConnection();
    }

    /**
     * 预提交，这里预提交的逻辑在invoke方法中
     */
    @Override
    protected void preCommit(Connection connection) throws Exception {
        logger.info("start preCommit...");
    }

    /**
     * 如果invoke方法执行正常，则提交事务
     */
    @Override
    protected void commit(Connection connection) {
        logger.info("start commit...");
        DBConnectUtil.commit(connection);
    }

    /**
     * 如果invoke执行异常则回滚事物，下一次的checkpoint操作也不会执行
     */
    @Override
    protected void abort(Connection connection) {
        logger.info("start abort rollback...");
        DBConnectUtil.rollback(connection);
    }
}

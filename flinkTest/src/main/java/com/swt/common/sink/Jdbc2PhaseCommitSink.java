package com.swt.common.sink;

import com.kye.utils.DruidConnectionPool;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Jdbc2PhaseCommitSink extends TwoPhaseCommitSinkFunction<Tuple2<String, Integer>, Jdbc2PhaseCommitSink.ConnectionState, Void> {
    private String insertSql;

    public Jdbc2PhaseCommitSink() {
        super(
                new KryoSerializer<>(Jdbc2PhaseCommitSink.ConnectionState.class, new ExecutionConfig())
                , VoidSerializer.INSTANCE
        );
    }

    @Override
    protected void invoke(ConnectionState transaction, Tuple2<String, Integer> value, Context context) throws Exception {
        Connection conn = transaction.connection;
        PreparedStatement pstm = conn.prepareStatement(insertSql);
        pstm.setString(1, value.f0);
        pstm.setInt(2, value.f1);
        pstm.setInt(3, value.f1);

        pstm.executeUpdate();
        pstm.close();
    }

    @Override
    protected ConnectionState beginTransaction() throws Exception {
        System.out.println("========> beginTransaction ... ");
        Connection conn = DruidConnectionPool.getConnection();
        conn.setAutoCommit(false);
        return new ConnectionState(conn);
    }

    @Override
    protected void preCommit(ConnectionState transaction) throws Exception {
        System.out.println("======> preCommit... " + transaction);
    }

    @Override
    protected void commit(ConnectionState transaction) {
        System.out.println("=========> commit ... ");
        Connection conn = transaction.connection;

        try {
            conn.commit();
            conn.close();
        } catch (SQLException e) {
            throw new RuntimeException("提交事务异常");
        }
    }

    @Override
    protected void abort(ConnectionState transaction) {
        System.out.println("=====> abort ... ");
        Connection conn = transaction.connection;
        try {
            conn.rollback();
            conn.close();
        } catch (SQLException e) {
            throw new RuntimeException("回滚事务异常");
        }
    }

    static class ConnectionState {
        private final transient Connection connection;

        ConnectionState(Connection conn) {
            this.connection = conn;
        }
    }

    public String getInsertSql() {
        return insertSql;
    }

    public void setInsertSql(String insertSql) {
        this.insertSql = insertSql;
    }
}

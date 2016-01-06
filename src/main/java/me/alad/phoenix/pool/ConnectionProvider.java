package me.alad.phoenix.pool;

import org.apache.phoenix.jdbc.PhoenixConnection;


/**
 * thrift连接池接口
 *
 */
public interface ConnectionProvider {
    /**
     * 获取连接池中的一个连接
     * 
     * @return
     */
    public PhoenixConnection getConnection();


    /**
     * 返回连接到连接池
     */
    public void returnConnection(PhoenixConnection conn);

}

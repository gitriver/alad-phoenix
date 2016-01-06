package me.alad.phoenix.pool;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * thrift连接管理类
 *
 */
public class ConnectionManager {

	/** 日志记录器 */
	public Logger logger = LoggerFactory.getLogger(ConnectionManager.class);
	
	/** 保存local对象 */
	public ThreadLocal<PhoenixConnection> socketThreadSafe = new ThreadLocal<PhoenixConnection>();

	/** 连接提供池 */
	public ConnectionProvider connectionProvider;

	/**
	 * 获取socket
	 * 
	 * @return TSocket
	 */
	public PhoenixConnection getConn() {
	    PhoenixConnection conn = null;
		try {
			conn = connectionProvider.getConnection();
			socketThreadSafe.set(conn);
			return socketThreadSafe.get();
		} catch (Exception e) {
			logger.error("ConnectionManager get phoenix connection fail", e);
		} 
		return conn;
	}
	
	/**
	 * 返回Socket
	 */
	public void closeConn(PhoenixConnection conn){
		connectionProvider.returnConnection(conn);
		socketThreadSafe.remove();
	}

	public ConnectionProvider getConnectionProvider() {
		return connectionProvider;
	}

	public void setConnectionProvider(ConnectionProvider connectionProvider) {
		this.connectionProvider = connectionProvider;
	}
}
package me.alad.phoenix.pool;

import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;


/**
 * Phoenix连接池接口实现类
 *
 */
public class ConnectionProviderImpl implements ConnectionProvider, InitializingBean, DisposableBean {

    public static final Logger logger = LoggerFactory.getLogger(ConnectionProviderImpl.class);

    /** 可以从缓存池中分配对象的最大数量 */
    private int maxActive = GenericObjectPool.DEFAULT_MAX_ACTIVE;
    /** 缓存池中最大空闲对象数量 */
    private int maxIdle = GenericObjectPool.DEFAULT_MAX_IDLE;
    /** 缓存池中最小空闲对象数量 */
    private int minIdle = GenericObjectPool.DEFAULT_MIN_IDLE;
    /** 阻塞时最大的等待时间 */
    private long maxWait = GenericObjectPool.DEFAULT_MAX_WAIT;

    /** 从缓存池中分配对象，是否执行PoolableObjectFactory.validateObject方法 */
    private boolean testOnBorrow = GenericObjectPool.DEFAULT_TEST_ON_BORROW;
    private boolean testOnReturn = GenericObjectPool.DEFAULT_TEST_ON_RETURN;
    private boolean testWhileIdle = GenericObjectPool.DEFAULT_TEST_WHILE_IDLE;

    /** 对象缓存池 */
    private ObjectPool<PhoenixConnection> objectPool = null;
    private PhoenixPoolableObjectFactory poolFactory;

    @Override
    public PhoenixConnection getConnection() {
        try {
            // 从对象池取对象
            PhoenixConnection conn = (PhoenixConnection) objectPool.borrowObject();
            return conn;
        }
        catch (Exception e) {
            throw new RuntimeException("Phoenix pool getConnection fail", e);
        }

    }


    @Override
    public void returnConnection(PhoenixConnection conn) {
        try {
            // 将对象放回对象池
            objectPool.returnObject(conn);
        }
        catch (Exception e) {
            throw new RuntimeException("Phoenix pool returnConnection fail", e);
        }
    }


    @SuppressWarnings("deprecation")
    @Override
    public void afterPropertiesSet() throws Exception {
        // 对象池
        objectPool = new GenericObjectPool<PhoenixConnection>();
        // 属性设置
        ((GenericObjectPool<PhoenixConnection>) objectPool).setMaxActive(maxActive);
        ((GenericObjectPool<PhoenixConnection>) objectPool).setMaxIdle(maxIdle);
        ((GenericObjectPool<PhoenixConnection>) objectPool).setMinIdle(minIdle);
        ((GenericObjectPool<PhoenixConnection>) objectPool).setMaxWait(maxWait);
        ((GenericObjectPool<PhoenixConnection>) objectPool).setTestOnBorrow(testOnBorrow);
        ((GenericObjectPool<PhoenixConnection>) objectPool).setTestOnReturn(testOnReturn);
        ((GenericObjectPool<PhoenixConnection>) objectPool).setTestWhileIdle(testWhileIdle);
        ((GenericObjectPool<PhoenixConnection>) objectPool)
            .setWhenExhaustedAction(GenericObjectPool.WHEN_EXHAUSTED_BLOCK);

        // 设置factory
        objectPool.setFactory(poolFactory);
    }


    /**
     * 关闭连接池
     */
    @Override
    public void destroy() throws Exception {
        try {
            objectPool.close();
            logger.info("Phoenix driver connectionPool closed because server is shutting down");
        }
        catch (Exception e) {
            logger.info("Phoenix driver connectionPool close error", e);
            throw new RuntimeException("erorr destroy()", e);
        }
    }


    /** getter setter */
    public int getMaxActive() {
        return maxActive;
    }


    public void setMaxActive(int maxActive) {
        this.maxActive = maxActive;
    }


    public int getMaxIdle() {
        return maxIdle;
    }


    public void setMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
    }


    public int getMinIdle() {
        return minIdle;
    }


    public void setMinIdle(int minIdle) {
        this.minIdle = minIdle;
    }


    public long getMaxWait() {
        return maxWait;
    }


    public void setMaxWait(long maxWait) {
        this.maxWait = maxWait;
    }


    public boolean isTestOnBorrow() {
        return testOnBorrow;
    }


    public void setTestOnBorrow(boolean testOnBorrow) {
        this.testOnBorrow = testOnBorrow;
    }


    public boolean isTestOnReturn() {
        return testOnReturn;
    }


    public void setTestOnReturn(boolean testOnReturn) {
        this.testOnReturn = testOnReturn;
    }


    public boolean isTestWhileIdle() {
        return testWhileIdle;
    }


    public void setTestWhileIdle(boolean testWhileIdle) {
        this.testWhileIdle = testWhileIdle;
    }


    public ObjectPool<PhoenixConnection> getObjectPool() {
        return objectPool;
    }


    public void setObjectPool(ObjectPool<PhoenixConnection> objectPool) {
        this.objectPool = objectPool;
    }
    
    public PhoenixPoolableObjectFactory getPoolFactory() {
        return poolFactory;
    }


    public void setPoolFactory(PhoenixPoolableObjectFactory poolFactory) {
        this.poolFactory = poolFactory;
    }
}

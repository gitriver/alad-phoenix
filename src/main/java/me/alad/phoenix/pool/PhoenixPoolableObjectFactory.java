package me.alad.phoenix.pool;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.lang.NumberUtils;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;


/**
 * phoenix连接工厂类
 *
 */
public class PhoenixPoolableObjectFactory implements PoolableObjectFactory<PhoenixConnection> {
    public static final Logger logger = LoggerFactory.getLogger(PhoenixPoolableObjectFactory.class);

    private String jdbcUrl;
    private int validateTimeout;
    private String tenantId;
    private String mutateMaxSize;
    private String hbaseConfFile;
    private String mutateBatchSize;


    /**
     * 创建TSocket对象 called whenever a new instance is needed
     */
    @Override
    public PhoenixConnection makeObject() throws Exception {
        return genConn();
    }


    /**
     * 检验对象是否可以由pool安全返回 invoked on activated instances to make sure they can be
     * borrowed from the pool. validateObject may also be used to test an
     * instance being returned to the pool before it is passivated. It will only
     * be invoked on an activated instance 返回False,调用destroyObject()。
     */
    @Override
    public boolean validateObject(PhoenixConnection obj) {

        PhoenixConnection conn = (PhoenixConnection) obj;
        // The time in seconds to wait for the database operation used to
        // validate the connection to complete
        try {
            if (conn.isValid(validateTimeout)) {
                return true;
            }
            else {
                return false;
            }
        }
        catch (SQLException e) {
            logger.info("validateObject fail:", e);
        }
        return false;
    }


    /**
     * 销毁对象 invoked on every instance when it is being "dropped" from the pool
     * (whether due to the response from validateObject, or for reasons specific
     * to the pool implementation.) There is no guarantee that the instance
     * being destroyed will be considered active, passive or in a generally
     * consistent state
     */
    @Override
    public void destroyObject(PhoenixConnection obj) throws Exception {
        if (obj instanceof PhoenixConnection) {
            PhoenixConnection conn = (PhoenixConnection) obj;
            if (!conn.isClosed()) {
                conn.close();
            }
        }
    }


    /**
     * 激活对象 invoked on every instance that has been passivated before it is
     * borrowed from the pool.
     */
    @Override
    public void activateObject(PhoenixConnection conn) throws Exception {
        if (conn.isClosed() || !conn.isValid(validateTimeout))
            conn = genConn();
    }


    /**
     * 钝化对象 invoked on every instance when it is returned to the pool
     */
    @Override
    public void passivateObject(PhoenixConnection obj) throws Exception {
        // TODO 暂不实现

    }


    private PhoenixConnection genConn() {
        PhoenixConnection pconn = null;
        try {
            Properties connProps = new Properties();
            connProps.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB, mutateMaxSize);
            connProps.setProperty(QueryServices.MUTATE_BATCH_SIZE_ATTRIB, mutateBatchSize);
            connProps.setProperty(QueryServices.MAX_SERVER_CACHE_SIZE_ATTRIB, "524288000");
            if (!Strings.isNullOrEmpty(tenantId))
                connProps.put(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);

            // 配置jdbc url,如果未配置或配置的为空，则从hbase.conf.file中获取jdbc url
            if (Strings.isNullOrEmpty(jdbcUrl)) {
                // 初始化HBaseConfiguration
                Configuration conf = HBaseConfiguration.create();
                conf.addResource(new Path(hbaseConfFile));
                jdbcUrl = QueryUtil.getConnectionUrl(connProps, conf);
                logger.info("phoenix jdbc url:" + jdbcUrl);
            }
            // 创建phoenix连接
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            pconn = DriverManager.getConnection(jdbcUrl, connProps).unwrap(PhoenixConnection.class);
            logger.info("获取Phoenix连接成功......");
            return pconn;

        }
        catch (Exception e) {
            logger.error("获取Phoenix连接失败:" + e.getMessage());
            logger.error("PhoenixPoolableObjectFactory makeObject fail,reason:", e.getMessage());
            throw new RuntimeException(e);
        }
    }


    /** getter setter */
    public String getJdbcUrl() {
        return jdbcUrl;
    }


    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }


    public int getValidateTimeout() {
        return validateTimeout;
    }


    public void setValidateTimeout(int validateTimeout) {
        this.validateTimeout = validateTimeout;
    }


    public String getTenantId() {
        return tenantId;
    }


    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }


    public String getMutateMaxSize() {
        return mutateMaxSize;
    }


    public void setMutateMaxSize(String mutateMaxSize) {
        this.mutateMaxSize = mutateMaxSize;
    }


    public String getHbaseConfFile() {
        return hbaseConfFile;
    }


    public void setHbaseConfFile(String hbaseConfFile) {
        this.hbaseConfFile = hbaseConfFile;
    }


    public String getMutateBatchSize() {
        return mutateBatchSize;
    }


    public void setMutateBatchSize(String mutateBatchSize) {
        this.mutateBatchSize = mutateBatchSize;
    }
    
    
}
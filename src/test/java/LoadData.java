import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import me.alad.phoenix.pool.ConnectionManager;
import me.alad.phoenix.pool.ConnectionProviderImpl;
import me.alad.phoenix.pool.PhoenixPoolableObjectFactory;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.util.CSVCommonsLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadData {
	public static char fieldDelimiter = 239; // 字符ï作为字段间的分隔符
	public static char quoteCharacter = 240;
	private static Logger LOG = LoggerFactory.getLogger(LoadData.class);
	public static ConnectionManager connManager = null;

	static {
		Properties props = new Properties();
		try {
			InputStream stream = LoadData.class.getClassLoader()
					.getResourceAsStream("phoenix.properties");
			props.load(stream);
			stream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		PhoenixPoolableObjectFactory ppf = new PhoenixPoolableObjectFactory();
		ppf.setJdbcUrl(props.getProperty("jdbcUrl"));
		ppf.setValidateTimeout(Integer.parseInt(props
				.getProperty("validateTimeout")));
		ppf.setMutateMaxSize(props.getProperty("mutateMaxSize"));
		ppf.setMutateBatchSize(props.getProperty("mutateBatchSize"));

		ConnectionProviderImpl cp = new ConnectionProviderImpl();
		cp.setPoolFactory(ppf);
		cp.setMaxActive(Integer.parseInt(props.getProperty("maxActive")));
		cp.setMaxIdle(Integer.parseInt(props.getProperty("maxIdle")));
		cp.setMinIdle(Integer.parseInt(props.getProperty("minIdle")));
		cp.setMaxWait(Integer.parseInt(props.getProperty("maxWait")));
		cp.setTestOnBorrow(Boolean.parseBoolean(props
				.getProperty("testOnBorrow")));
		cp.setTestOnReturn(Boolean.parseBoolean(props
				.getProperty("testOnReturn")));
		cp.setTestWhileIdle(Boolean.parseBoolean(props
				.getProperty("testWhileIdle")));
		try {
			cp.afterPropertiesSet();
		} catch (Exception e) {
			e.printStackTrace();
		}

		connManager = new ConnectionManager();
		connManager.setConnectionProvider(cp);
	}

	/**
	 * 将数据写入phoenix
	 * 
	 * @param connManager
	 * @param tableName
	 * @param fieldNames
	 * @param fileName
	 */
	public static void dumpToPhoenix(String tableName, List<String> fields,
			String fileName) {
		PhoenixConnection conn = connManager.getConn();
		CSVCommonsLoader loader = new CSVCommonsLoader(conn,
				tableName.toUpperCase(), fields, false, fieldDelimiter,
				quoteCharacter, null, ":");
		long start = System.currentTimeMillis();
		try {
			loader.upsert(fileName);
		} catch (Exception e) {
			LOG.info("加载文件{}到表{}失败", fileName, tableName.toUpperCase());
			e.printStackTrace();
			return;
		} finally {
			try {
				conn.commit();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			connManager.closeConn(conn);
		}
		long end = System.currentTimeMillis();
		LOG.info("加载文件{}到表{}成功,耗时{}ms", fileName, tableName.toUpperCase(),
				(end - start));
	}

	public static void main(String[] args) {
		System.out.println("==="+args.length);
		
		String s = args[0];
		String tableName = args[1];
		String file = args[2];
		// String s =
		// "taskid,busidate,resid,clusterno,cateid,catename,keywords,hotvalue,insertdate";
		String[] datas = s.split(",");
		List<String> fields = new ArrayList<String>();
		for (String string : datas) {
			fields.add(string.toUpperCase());
		}
		// dumpToPhoenix("cls.result".toUpperCase(), fields,
		// "C:\\Users\\Administrator\\Downloads\\clsResult.txt");
		dumpToPhoenix(tableName.toUpperCase(), fields, file);
	}
}


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import me.alad.phoenix.pool.ConnectionManager;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.util.CSVCommonsLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Test {
	private static Logger LOG = LoggerFactory.getLogger(Test.class);
	public static char fieldDelimiter = 239; // 字符ï作为字段间的分隔符
	public static char quoteCharacter = 240;

	public static void main(String[] args) {

		if (args.length != 3) {
			System.out.println("==");
			System.exit(0);
		}

		List<String> fields = new ArrayList<String>();

		String[] info = args[2].split(",");
		for (String string : info) {
			fields.add(string.toUpperCase());
		}

		ApplicationContext context = new ClassPathXmlApplicationContext(
				"applicationContext-phoenix.xml");

		ConnectionManager connManager = (ConnectionManager) context
				.getBean("connectionManagerBean");

		PhoenixConnection conn = connManager.getConn();

		dumpToPhoenix(connManager, args[0], args[1], fields);

		connManager.closeConn(conn);
	}

	/**
	 * 将数据写入phoenix
	 * 
	 * @param connManager
	 * @param tableName
	 * @param fieldNames
	 * @param fileName
	 */
	public static void dumpToPhoenix(ConnectionManager connManager,
			String fileName, String tableName, List<String> fields) {
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
		System.out.println("加载文件" + fileName + "到表" + tableName.toUpperCase()
				+ "成功,耗时" + (end - start) + "ms");
	}
}

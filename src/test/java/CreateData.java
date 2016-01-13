import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

public class CreateData {
	public static char fieldDelimiter = 239; // 字符ï作为字段间的分隔符
//	public static String fieldDelimiter = ",";

	public static void main(String[] args) throws Exception {
		String filePath = "D:/systex/hbase.txt";
		if (args.length > 0) {
			filePath = args[0];
		}

		File file = new File(filePath);
		PrintWriter pw = new PrintWriter(new OutputStreamWriter(
				new BufferedOutputStream(new FileOutputStream(file))));

		int num = 1;
		while (true) {

			if (file.length() >= 1024) {
				break;
			}

			pw.println("test" + num + fieldDelimiter + "test" + num
					+ fieldDelimiter + "test" + num + fieldDelimiter + "test"
					+ num + fieldDelimiter + "test" + num + fieldDelimiter
					+ "test" + num);
			num++;
		}

		pw.close();

	}
}

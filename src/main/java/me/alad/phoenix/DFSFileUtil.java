package me.alad.phoenix;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;


public class DFSFileUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DFSFileUtil.class);

    public static final int BUFFER_SIZE = 0;

    public static FileSystem fs;
    public static Configuration conf;

    // 初始化环境
    static {
        // Properties props = getProperties("hadoop.properties");

        conf = new Configuration();
        conf.addResource(new Path("F:\\hdfs-site.xml"));
        conf.addResource(new Path("F:\\hbase-site.xml"));
        conf.set("fs.defaultFS", "hdfs://ju51nn");
        try {
            String ugi = "";
            if (Strings.isNullOrEmpty(ugi)){
                fs = FileSystem.get(conf);
                System.out.println("======"+ fs) ;
            }
            else {
                UserGroupInformation.createProxyUser(ugi, UserGroupInformation.getLoginUser()).doAs(
                    new PrivilegedExceptionAction<Void>() {
                        @Override
                        public Void run() throws Exception {
                            fs = FileSystem.get(conf);
                            return null;
                        }

                    });
            }
        }
        catch (Exception e) {
            LOG.error("初始化FileSytem对象异常: ", e.getMessage());
            e.printStackTrace();
        }
    }


    public static Properties getProperties(String propFile) {
        Properties props = new Properties();
        try {
            InputStream stream = DFSFileUtil.class.getClassLoader().getResourceAsStream(propFile);
            props.load(stream);
            stream.close();
        }
        catch (FileNotFoundException e) {
            LOG.error("解析数据库配置文件:No such file " + propFile);
        }
        catch (Exception e1) {
            e1.printStackTrace();
            LOG.error("解析数据库配置文件:" + e1.getMessage());
        }
        return props;
    }


    /**
     * 判断文件或目录是否存在
     * 
     * @param file
     * @return
     */
    public static boolean exists(String file) {
        try {
            return fs.exists(new Path(file));
        }
        catch (Exception e) {
            LOG.error("创建目录操作出错: ", e.getMessage());
            e.printStackTrace();
        }
        return false;
    }


    /**
     * 创建目录
     * 
     * @param dir
     *            目录
     */
    public static boolean mkdir(String dir) {
        try {
            Path dirPath = new Path(dir);
            if (!fs.exists(dirPath)) {
                return fs.mkdirs(dirPath);
            }
        }
        catch (Exception e) {
            LOG.error("创建目录操作出错: ", e.getMessage());
            e.printStackTrace();
        }
        return false;
    }


    /**
     * 创建目录及其上级目录
     * 
     * @param path
     *            目录路径
     */
    public static boolean mkdirs(String dirs) {
        return mkdir(dirs);
    }


    /**
     * 删除文件夹
     * 
     * @param dir_path
     *            文件夹的path
     */
    public static boolean deldir(String dir_path, boolean recursive) {
        Path dirPath = new Path(dir_path);
        try {
            if (fs.exists(dirPath))
                return fs.delete(dirPath, recursive);
        }
        catch (IOException e) {
            LOG.error("删除目录操作出错: ", e.getMessage());
            e.printStackTrace();
        }
        return false;
    }


    /**
     * 　 新建单个文件
     * 
     * @param filePath
     */
    public static boolean createNewFile(String filePath) {
        try {
            Path path = new Path(filePath);
            return fs.createNewFile(path);
        }
        catch (Exception e) {
            LOG.error("新建文件操作出错: " + e.getMessage());
            e.printStackTrace();
        }
        return false;
    }


    /**
     * 新建文件并写内容
     * 
     * @param filePath
     *            包含路径的文件名 如:/aa/bb.txt
     * @param content
     *            文件内容
     * 
     */
    public static void createNewFile(String filePath, String content) {
        PrintWriter pw = null;
        try {
            createNewFile(filePath);
            pw = new PrintWriter(fs.create(new Path(filePath)));
            pw.println(content);
            pw.flush();
            pw.close();
        }
        catch (Exception e) {
            LOG.error("新建文件操作出错: " + e.getMessage());
            e.printStackTrace();
        }
        finally {
            pw.close();
        }
    }


    /**
     * 重命名单个文件
     * 
     * @param filePath
     *            包含路径的文件名
     */
    public static void renameFile(String srcFilePath, String targetFilePath) {
        try {
            fs.rename(new Path(srcFilePath), new Path(targetFilePath));
        }
        catch (IllegalArgumentException | IOException e) {
            LOG.error("重命名文件操作出错: " + e.getMessage());
            e.printStackTrace();
        }
    }


    /**
     * 删除单个文件
     * 
     * @param filePath
     *            包含路径的文件名
     */
    public static void delFile(String filePath) {
        try {
            fs.delete(new Path(filePath), false);
        }
        catch (Exception e) {
            LOG.error("删除文件操作出错: " + e.getMessage());
            e.printStackTrace();
        }
    }


    public static boolean chmod(String filePath) {
        return chmod(filePath, "drwxrwxrwx", true);
    }


    public static boolean chmod(String filePath, String chmodStr, boolean recursive) {
        return chmod(new Path(filePath), chmodStr, recursive);
    }


    public static boolean chmod(Path path, String chmodStr, boolean recursive) {
        boolean result = false;
        try {
            fs.setPermission(path, FsPermission.valueOf(chmodStr));
            if (recursive == true) {
                FileStatus stats[] = fs.listStatus(path);
                for (FileStatus stat : stats) {
                    Path subPath = stat.getPath();
                    fs.setPermission(subPath, FsPermission.valueOf(chmodStr));
                    if (fs.isDirectory(subPath)) {
                        chmod(subPath, chmodStr, recursive);
                    }
                }
            }
        }
        catch (Exception e) {
            LOG.error("修改文件权限出错: " + e.getMessage());
            e.printStackTrace();
        }
        return result;
    }


    public static boolean chmod(String filePath, String chmodStr) {
        boolean result = false;
        Path path = new Path(filePath);
        try {
            FileStatus stats[] = fs.listStatus(path);
            for (FileStatus stat : stats) {
                Path subPath = stat.getPath();
                fs.setPermission(path, FsPermission.valueOf(chmodStr));
            }
            result = true;
        }
        catch (Exception e) {
            LOG.error("修改文件权限出错: " + e.getMessage());
            e.printStackTrace();
        }
        return result;
    }


    /**
     * 得到单个文件大小
     * 
     * @param filePath
     */
    public static long getFileSize(String filePath) {
        try {
            return fs.getFileStatus(new Path(filePath)).getLen();
        }
        catch (IllegalArgumentException e) {
            LOG.error("获取文件大小出错: " + e.getMessage());
            e.printStackTrace();
        }
        catch (IOException e) {
            LOG.error("获取文件大小出错: " + e.getMessage());
            e.printStackTrace();
        }
        return -1;
    }


    /**
     * 复制单个本地文件至hdfs
     * 
     * @param srcFilePath
     *            包含路径的源文件 如：/abc.txt
     * @param destFilePath
     *            目标文件目录；若文件目录不存在则自动创建 如：/dest
     * @throws IOException
     */
    public static boolean copyFile(String srcFilePath, String destFilePath) {
        boolean result = false;
        File file = new File(srcFilePath);
        if (file.length() == 0) {
            return result;
        }
        try {
            fs.copyFromLocalFile(new Path(srcFilePath), new Path(destFilePath));
            result = true;
        }
        catch (Exception e1) {
            LOG.error("读取文件失败:" + e1.getMessage());
            e1.printStackTrace();
        }
        return result;
    }


    /**
     * 复制本地一个文件夹至hdfs
     * 
     * @param oldPath
     *            String 源文件夹路径 如：/src
     * @param newPath
     *            String 目标文件夹路径 如：/dest
     */
    public static void copyFolder(String oldPath, String newPath) {
        try {
            mkdir(newPath);
            File file = new File(oldPath);
            String[] files = file.list();
            String tempPath = null;
            File tempFile = null;
            for (int i = 0; i < files.length; i++) {
                if (oldPath.endsWith(File.separator)) {
                    tempPath = oldPath + files[i];
                }
                else {
                    tempPath = oldPath + File.separator + files[i];
                }
                tempFile = new File(tempPath);
                if (tempFile.isFile()) {
                    copyFile(tempPath, newPath + "/" + files[i]);
                }
                if (tempFile.isDirectory()) {// 如果是子文件夹
                    copyFolder(oldPath + "/" + files[i], newPath + "/" + files[i]);
                }
            }
        }
        catch (Exception e) {
            LOG.error("复制文件夹操作出错:" + e.getMessage());
            e.printStackTrace();
        }
    }


    /**
     * 读取文件内容
     * 
     * @param filePath
     *            文件路径
     */
    public static String readFile(String filePath) {
        StringBuffer sb = new StringBuffer();
        try {
            FSDataInputStream is = fs.open(new Path(filePath));
            int byteread = 0;
            byte[] tempbytes = new byte[BUFFER_SIZE];
            while ((byteread = is.read(tempbytes)) != -1) {
                sb.append(new String(tempbytes, 0, byteread));
            }
            if (is != null) {
                is.close();
            }
        }
        catch (Exception e1) {
            LOG.error("读取文件失败:" + e1.getMessage());
            e1.printStackTrace();
        }
        return sb.toString();
    }


    /**
     * 以行为单位读取文件
     * 
     * @param filePath
     * @return
     */
    public static List<String> readFileByLines(String filePath) {
        return readFileByLines(filePath, -1, -1);
    }


    /**
     * 取文件行数
     * 
     * @param filePath
     * @return
     */
    public static int getFileLineCount(String filePath) {
        return readFileByLines(filePath).size();
    }


    /**
     * 按行号为单位按行读取文件，行号以1开始，-1代表无限小或无限大
     * 
     * @param filePath
     *            文件路径
     * @param beginIndex
     *            开始行号
     * @param endIndex
     *            结束行号
     * @return
     */
    public static List<String> readFileByLines(String filePath, int beginIndex, int endIndex) {
        List<String> list = new ArrayList<>();
        BufferedReader br = null;
        FSDataInputStream is = null;
        try {
            LOG.info("以行为单位读取文件内容，一次读一整行：");
            is = fs.open(new Path(filePath));
            br = new BufferedReader(new InputStreamReader(is), BUFFER_SIZE);
            String tempString = null;
            int lineindex = 0;
            if (endIndex == -1) {
                while ((tempString = br.readLine()) != null) {
                    lineindex++;
                    if (lineindex >= beginIndex)
                        list.add(tempString);
                }
            }
            else {
                while ((tempString = br.readLine()) != null) {
                    lineindex++;
                    if ((lineindex >= beginIndex) && (lineindex <= endIndex))
                        list.add(tempString);
                }
            }
            if (is != null) {
                is.close();
            }
            if (br != null) {
                br.close();
            }
        }
        catch (IOException e) {
            LOG.error("读取文件失败:" + e.getMessage());
            e.printStackTrace();
        }
        return list;
    }


    /**
     * 以行为单位读取文件
     * 
     * @param filePath
     * @return
     */
    public static List<String> readFileByLinesNoDup(String filePath) {
        return readFileByLinesNoDup(filePath, -1, -1);
    }


    /**
     * 按行号为单位按行读取文件，行号以1开始，-1代表无限小或无限大
     * 
     * @param filePath
     *            文件路径
     * @param beginIndex
     *            开始行号
     * @param endIndex
     *            结束行号
     * @return
     */
    public static List<String> readFileByLinesNoDup(String filePath, int beginIndex, int endIndex) {
        Set<String> set = new HashSet<String>();
        BufferedReader br = null;
        FSDataInputStream is = null;
        try {
            LOG.info("以行为单位读取文件内容，一次读一整行：");
            is = fs.open(new Path(filePath));
            br = new BufferedReader(new InputStreamReader(is), BUFFER_SIZE);
            String tempString = null;
            int lineindex = 0;
            if (endIndex == -1) {
                while ((tempString = br.readLine()) != null) {
                    lineindex++;
                    if (lineindex >= beginIndex)
                        set.add(tempString);
                }
            }
            else {
                while ((tempString = br.readLine()) != null) {
                    lineindex++;
                    if ((lineindex >= beginIndex) && (lineindex <= endIndex))
                        set.add(tempString);
                }
            }
            if (is != null) {
                is.close();
            }
            if (br != null) {
                br.close();
            }
        }
        catch (IOException e) {
            LOG.error("读取文件失败:" + e.getMessage());
            e.printStackTrace();
        }
        List<String> list = new ArrayList<String>(set.size());
        list.addAll(set);
        return list;
    }


    /**
     * 写文件
     * 
     * @param content
     *            写入内容
     * @param filePath
     *            写入的文件
     * @throws Exception
     */
    public static void writeFile(String filePath, String content) {
        PrintWriter pw;
        Path path = new Path(filePath);
        try {
            pw = new PrintWriter(fs.create(path, true));
            pw.write(content);
            pw.flush();
            if (pw != null) {
                pw.close();
            }
        }
        catch (IOException e) {
            LOG.error("写文件失败:" + e.getMessage());
            e.printStackTrace();
        }
    }


    /**
     * 文件名是否采用日期格式
     * 
     * @param filePath
     * @param list
     * @param isDateFormat
     */
//    public static void writeFileByLines(String filePath, String fileName, List<String> list) {
//        String tmp = genFileNameWithDate(fileName);
//        Path path = new Path(filePath + File.separator + tmp);
//        writeFileByLines(path, list);
//    }


    public static void writeFileByLines(String filePath, List<String> list) {
        writeFileByLines(new Path(filePath), list);
    }


    /**
     * 按行写文件List
     * 
     * @param filePath
     *            文件路径
     * @param list
     *            写入内容的String 列表
     */
    public static void writeFileByLines(Path path, List<String> list) {
        PrintWriter pw = null;
        FSDataOutputStream os = null;
        try {
            os = fs.create(path, true);
            pw = new PrintWriter(os);
            Iterator<String> it = list.iterator();
            int index = 0;
            while (it.hasNext()) {
                String str = it.next();
                pw.println(str);
                index++;
                if (index % 5000 == 0) {
                    pw.flush();
                }
            }
            pw.flush();
            if (os != null) {
                os.close();
            }
            if (pw != null) {
                pw.close();
            }
        }
        catch (Exception e) {
            LOG.error("写文件异常:" + e.getMessage());
            e.printStackTrace();
        }
    }


    public static void createFile(String path) {
        createFile(new Path(path));
    }


    /*
     * 创建文件,如果指定的路径不存在,则创建
     */
    public static void createFile(Path path) {
        try {
            fs.createNewFile(path);
        }
        catch (IOException e) {
            LOG.error("创建文件操作出错:" + e.getMessage());
        }
    }


    /**
     * 根据当前日期产生文件名
     * 
     * @param fileName
     * @return
     */
//    public static String genFileNameWithDate(String prefix) {
////        String date = DateUtil.parseToString(new Date(), "yyyy-MM-dd");
//        return prefix + date + ".log";
//    }


    public static void main(String args[]) throws IOException {
        copyFile("D:/tmp/bb.jpg", "hdfs://172.16.20.244/tmp/bb20150121.jpg");
    }
}

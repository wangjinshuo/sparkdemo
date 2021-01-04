import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class HdfsOp {
    public static String serverIP = "127.0.0.1" ;
    public static String serverPort = "9000" ;

    public  static void main(String[] args) throws Exception {
        String windowsFile = "/root/Desktop/token/test.txt" ;
        String hdfsFile = "/2021/test.txt" ;
        //上传文件
        //1： 测试本地文件不存在   2.测试HDFS文件存在   3.测试正常
        PutFile(windowsFile,hdfsFile);

        //下载文件
        //1: 测试本地文件存在   2.测试HDFS文件不存在   3.测试正常
//        GetFile(windowsFile, hdfsFile);

//        删除文件
//        1. 测试HDFS文件不存在  2.测试DHFS文件存在
//        DeleteFile( hdfsFile);
    }

    /**
     * 删除HDFS文件
     */
    private static void DeleteFile(String hdfsFile) throws IOException {
        //创建一个配置对象
        Configuration conf = new Configuration();
        String confString = "hdfs://" + serverIP + ":" + serverPort ;
        //core-site.xml 里面的内容
        conf.set("fs.defaultFS", confString);
        //获取操作HDFS的对象
        FileSystem fileSystem = FileSystem.get(conf);
        //如果要删除目录，第二个参数应设置为true,表示允许递归删除
        boolean deleteFlag = fileSystem.delete(new Path(hdfsFile),true);
        if(deleteFlag) {
            System.out.println("删除成功!");
        }
        else{
            System.out.println("删除失败!");
        }
    }

    /**
     * 从Windws上下载HDFS文件
     */
    private static void GetFile(String windowsFile, String hdfsFile) throws IOException {
        //创建一个配置对象
        Configuration conf = new Configuration();
        String confString = "hdfs://" + serverIP + ":" + serverPort ;
        //core-site.xml 里面的内容
        conf.set("fs.defaultFS", confString);
        //获取操作HDFS的对象
        FileSystem fileSystem = FileSystem.get(conf);
        //判断HDFS是否存在,如果不存在则抛出异常
        if (!fileSystem.exists(new Path(hdfsFile))) {
            // 文件不存在
            throw new RuntimeException("HDFS文件不存在！<错误来自：HdfsOP.GetFile函数>");
        }
        File file = new File(windowsFile);
        //判断windowsfile是否存在，如果存在删除它
        if(file.exists()) {
            // 文件存在,删除
            file.delete();
        }
        //获取HDFS文件系统中的输入流
        FSDataInputStream fis = fileSystem.open(new Path(hdfsFile));
        //获取本地文件输出流
        FileOutputStream fos = new FileOutputStream(windowsFile);
        IOUtils.copyBytes(fis,fos,1024,true);
    }

    /**
     * 从Windows上传HDFS文件
     */
    private static void PutFile(String inputFile,String hdfsFile) throws IOException {
        //创建一个配置对象
        Configuration conf = new Configuration();
        String confString = "hdfs://" + serverIP + ":" + serverPort ;
        //core-site.xml 里面的内容
        conf.set("fs.defaultFS", confString);
        //获取操作HDFS的对象
        FileSystem fileSystem = FileSystem.get(conf);
        //首先判断本地文件是否存在
        File file = new File(inputFile);
        if(!file.exists()) {
            // 文件不存在
            throw new RuntimeException("Windows文件不存在！<错误来自：HdfsOP.PutFile函数>");
        }
        //判断HDFS是否存在,如果存在则删除它
        if (fileSystem.exists(new Path(hdfsFile))) {
            //调用类自身方法删除已存在文件
            fileSystem.delete(new Path(hdfsFile),true);
        }
        //开始上传文件
        //获取本地文件的输入流
        FileInputStream fis = new FileInputStream(inputFile) ;
        //获取HDFS的输出流
        FSDataOutputStream fos = fileSystem.create(new Path(hdfsFile));
        //通过Hadoop IO工具类把本地文件流拷贝到 HDFS文件系统里去
        IOUtils.copyBytes(fis,fos,1024,true);
    }

    public void setServerIP(String ip){
        this.serverIP = ip ;

    }
    public void setServerPort(String port){
        this.serverPort = port ;

    }
    public String getServerIP(String ip){
        return this.serverIP;

    }
    public String getServerPort(String port){
        return this.serverPort;

    }
}

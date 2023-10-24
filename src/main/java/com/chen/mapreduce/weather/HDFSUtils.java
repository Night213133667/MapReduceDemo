package com.chen.mapreduce.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 * ClassName: HDFSUtils
 * Package: com.chen.mapreduce.weather
 * Description:
 *
 * @Author: Night
 * @Create: 2023/10/15 - 8:52
 * @Version: 1.0
 */
public class HDFSUtils {
    /**
     * 获取FileSystem对象
     * @return
     */
    public static FileSystem getFileSystem() {
        // 连接HDFS
        Configuration configuration = new Configuration(); // 获取配置文件对象
        configuration.set("fs.defaultFS", "hdfs://192.168.235.129:9000");

        // 调用HDFS下的对象：FileSystem - 操作HDFS的对象
        FileSystem fileSystem = null;
        try {
            if (fileSystem == null) {
                fileSystem = FileSystem.get(configuration);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return fileSystem;
    }


    /**
     * 释放FileSystem对象
     * @param fileSystem
     */
    public static void ClosedFileSystem(FileSystem fileSystem) {
        try {
            if (fileSystem != null) {
                fileSystem.close();
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}



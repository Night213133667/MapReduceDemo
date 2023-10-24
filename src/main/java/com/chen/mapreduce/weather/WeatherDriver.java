package com.chen.mapreduce.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * ClassName: WeatherDriver
 * Package: com.chen.mapreduce.weather
 * Description:
 *
 * @Author: Night
 * @Create: 2023/10/14 - 19:27
 * @Version: 1.0
 */
public class WeatherDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1.获取job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //2.设置jar包
        job.setJarByClass(WeatherDriver.class);

        //3.关联mapper和reducer
        job.setMapperClass(WeatherMapper.class);
        job.setReducerClass(WeatherReducer.class);

        //4.设置map的输出的KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.设置最终输出的KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6.设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path("D:\\Desktop\\homework\\weather_forecast_source"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\Desktop\\homework\\output2"));

        //
        FileSystem fileSystem = HDFSUtils.getFileSystem();
        Path path = new Path("/MapReduceOut");
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
            System.out.println("删除成功");
        }
        FileOutputFormat.setOutputPath(job, path);

        // 提交任务
        boolean waitForCompletion = job.waitForCompletion(true);
        System.out.println(waitForCompletion ? "执行成功" : "执行失败");
        System.exit(waitForCompletion? 0 : 1);
    }

}

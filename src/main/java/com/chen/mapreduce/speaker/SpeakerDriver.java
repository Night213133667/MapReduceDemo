package com.chen.mapreduce.speaker;

import com.chen.mapreduce.tempetature.Temperature;
import com.chen.mapreduce.tempetature.TemperatureDriver;
import com.chen.mapreduce.tempetature.TemperatureMapper;
import com.chen.mapreduce.tempetature.TemperatureReducer;
import com.chen.mapreduce.weather.HDFSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * ClassName: Speaker
 * Package: com.chen.mapreduce.speaker
 * Description:
 *
 * @Author: Night
 * @Create: 2023/10/18 - 16:30
 * @Version: 1.0
 */
public class SpeakerDriver{
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        //1.获取job对象
        Job job = Job.getInstance(configuration, "KeyWords");
        //2.设置jar包
        job.setJarByClass(SpeakerDriver.class);
        //3.关联mapper和reducer
        job.setMapperClass(SpeakerMapper.class);
        job.setCombinerClass(SpeakerCombiner.class);
        job.setReducerClass(SpeakerReducer.class);
        //4.设置map的输出的KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //5.设置最终输出的KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 第一步：设置读取文件的类：K1和V1
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //6.设置输入输出路径
//        TextInputFormat.addInputPath(job, new Path("D:\\Desktop\\homework\\KeyWords"));
//        TextInputFormat.addInputPath(job, new Path("hdfs://master:9000/input/weather_forecast_source"));
        TextInputFormat.addInputPath(job, new Path(args[0]));
//        TextOutputFormat.setOutputPath(job, new Path("D:\\Desktop\\homework\\output4"));
//        TextOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/output"));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        //提交作业
        boolean waitForCompletion = job.waitForCompletion(true);
        System.out.println(waitForCompletion ? "执行成功" : "执行失败");
        System.exit(waitForCompletion? 0 : 1);
    }
}


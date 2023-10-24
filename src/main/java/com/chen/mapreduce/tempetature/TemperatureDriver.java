package com.chen.mapreduce.tempetature;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * ClassName: TemperatureDriver
 * Package: com.chen.mapreduce.tempetature
 * Description:
 *
 * @Author: Night
 * @Create: 2023/10/15 - 9:15
 * @Version: 1.0
 */
public class TemperatureDriver{
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //1.获取job对象
        Job job = Job.getInstance(configuration, "mapreduce_temperature");
        //2.设置jar包
        job.setJarByClass(TemperatureDriver.class);
        //3.关联mapper和reducer
        job.setMapperClass(TemperatureMapper.class);
        job.setReducerClass(TemperatureReducer.class);
        //4.设置map的输出的KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //5.设置最终输出的KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Temperature.class);
        // 第一步：设置读取文件的类：K1和V1
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //6.设置输入输出路径
//        TextInputFormat.addInputPath(job, new Path("D:\\Desktop\\homework\\weather_forecast_source"));
//        TextInputFormat.addInputPath(job, new Path("hdfs://master:9000/input/weather_forecast_source"));
        TextInputFormat.addInputPath(job, new Path(args[0]));
//        TextOutputFormat.setOutputPath(job, new Path("D:\\Desktop\\homework\\output3"));
//        TextOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/output"));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        //提交作业
        boolean waitForCompletion = job.waitForCompletion(true);
        System.out.println(waitForCompletion ? "执行成功" : "执行失败");
        System.exit(waitForCompletion? 0 : 1);
    }
}

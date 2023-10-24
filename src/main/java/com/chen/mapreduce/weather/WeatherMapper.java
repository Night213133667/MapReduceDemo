package com.chen.mapreduce.weather;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * ClassName: WeatherMapper
 * Package: com.chen.mapreduce.weather
 * Description:
 *
 * @Author: Night
 * @Create: 2023/10/14 - 19:22
 * @Version: 1.0
 */
public class WeatherMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text outKey = new Text();
    private IntWritable outValue = new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 获取到文件名对应的InputSplit
        InputSplit inputSplit = context.getInputSplit();
        // 强转成子类类型FileSplit
        FileSplit fSplit = (FileSplit) inputSplit;
        // 获取到路径
        Path path = fSplit.getPath();
        // 获取到文件名
        String name = path.getName();

        // 1、获取一行
        String line = value.toString();

        // 2、截取
        String[] split = line.split(" +");

        // 3、输出到上下文对象中
        outKey.set(name);
        int valueOf = Integer.valueOf(split[4].trim());
        // 由于数据文件中存在脏数据，所以要进行判断处理
        if (valueOf != -9999) {
            outValue.set(valueOf);
            context.write(outKey, outValue);
        }
    }
}

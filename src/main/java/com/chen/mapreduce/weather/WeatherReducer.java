package com.chen.mapreduce.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * ClassName: WeatherReducer
 * Package: com.chen.mapreduce.weather
 * Description:
 *
 * @Author: Night
 * @Create: 2023/10/14 - 19:25
 * @Version: 1.0
 */
public class WeatherReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable val = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 累加
        int sum = 0; // 存储累加后的值
        int count = 0; // 统计数据的个数
        for (IntWritable intWritable : values) {
            sum += intWritable.get();
            count++;
        }
        sum /= count;

        // 输出
        val.set(sum);
        context.write(key, val);
    }
}

package com.chen.mapreduce.tempetature;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * ClassName: TemperatureReducer
 * Package: com.chen.mapreduce.tempetature
 * Description:
 *
 * @Author: Night
 * @Create: 2023/10/15 - 9:08
 * @Version: 1.0
 */
public class TemperatureReducer extends Reducer<Text, LongWritable, Text, Temperature> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long maxTemperature = Long.MIN_VALUE;
        long minTemperature = Long.MAX_VALUE;
        double avgTemperature = 0.0;
        long temp;
        int count = 0;
        if (values!=null) {
            for (LongWritable value: values) {
                temp = value.get();
                maxTemperature = Math.max(temp, maxTemperature);
                minTemperature = Math.min(temp, minTemperature);
                avgTemperature += temp;
                count++;
            }
            Temperature temperature = new Temperature(maxTemperature, minTemperature, avgTemperature/count);

            context.write(key, temperature);
        }

    }
}

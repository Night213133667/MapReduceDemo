package com.chen.mapreduce.speaker;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * ClassName: SpeakerCombiner
 * Package: com.chen.mapreduce.speaker
 * Description:
 *
 * @Author: Night
 * @Create: 2023/10/18 - 16:28
 * @Version: 1.0
 */
public class SpeakerCombiner extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        // 合并
        int sum = 0;
        for (Text text : value) {
            Integer valueOf = Integer.valueOf(text.toString());
            sum += valueOf;
        }

        // 改变kv重新输出
        String[] split = key.toString().split(":");
        String outKey = split[0]; // 单词
        String outValue = split[1] + ":" + split[2] + ":" + String.valueOf(sum); // 文件名:辩论者:出现次数

        context.write(new Text(outKey), new Text(outValue));
    }
}


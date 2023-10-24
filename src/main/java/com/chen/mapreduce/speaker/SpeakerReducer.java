package com.chen.mapreduce.speaker;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * ClassName: SpeakerReducer
 * Package: com.chen.mapreduce.speaker
 * Description:
 *
 * @Author: Night
 * @Create: 2023/10/18 - 16:30
 * @Version: 1.0
 */
public class SpeakerReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        StringBuffer stringBuffer = new StringBuffer();
        for (Text text : value) {
            stringBuffer.append(text.toString() + "\t");
        }

        context.write(key, new Text(stringBuffer.toString()));
    }
}

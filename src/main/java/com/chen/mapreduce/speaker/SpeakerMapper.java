package com.chen.mapreduce.speaker;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


import java.io.IOException;
import java.util.StringTokenizer;

/**
 * ClassName: SpeakerMapper
 * Package: com.chen.mapreduce.speaker
 * Description:
 *
 * @Author: Night
 * @Create: 2023/10/18 - 16:26
 * @Version: 1.0
 */
public class SpeakerMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        // 获取按指定规则分割后的一行
        String[] split = value.toString().split(",");

        if (split.length == 4 && Character.isDigit(split[0].charAt(0))) {
            String speaker = split[1]; // 辩论者
            String text = split[2]; // 辩论内容

            // 对辩论内容再进行分割
            /**
             * 字符串StringTokenizer类允许应用程序将字符串拆分成令牌。
             * StringTokenizer方法不区分标识符，数字和引用的字符串，也不识别和跳过注释。
             * 可以在创建时或每个令牌的基础上指定一组分隔符（分隔标记的字符）。
             */
            StringTokenizer stringTokenizer = new StringTokenizer(text, " (),.?!--\"\"\n#");

            // 获取文件名
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

            // 遍历每一个单词
            while (stringTokenizer.hasMoreElements()) {
                String nextToken = stringTokenizer.nextToken();
                outKey.set(nextToken + ":" + fileName + ":" + speaker);
                outValue.set("1");
                context.write(outKey, outValue);
            }
        }
    }
}


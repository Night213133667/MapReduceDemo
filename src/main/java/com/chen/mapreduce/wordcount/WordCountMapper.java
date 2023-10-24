package com.chen.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ClassName: WordCountMapper
 * Package: com.chen.mapreduce.wordcount
 * Description:
 *
 * @Author: Night
 * @Create: 2023/10/13 - 20:10
 * @Version: 1.0
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    private Text outK = new Text();
    private IntWritable outV = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //切割
        String[] words = line.split(" ");
        //循环写出

        for (String word : words){
            //封装
            outK.set(word);
            //写出
            context.write(outK,outV);
        }
    }
}

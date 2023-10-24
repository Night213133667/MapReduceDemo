package com.chen.mapreduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * ClassName: FlowReducer
 * Package: com.chen.mapreduce.writable
 * Description:
 *
 * @Author: Night
 * @Create: 2023/10/14 - 10:52
 * @Version: 1.0
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private FlowBean outV = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        long totalUp = 0;
        long totalDown = 0;
        //1.遍历集合，累加值
        for (FlowBean value : values){
            totalUp += value.getUpFlow();
            totalDown += value.getDownFlow();
        }

        //2.封装outK,outV
        outV.setUpFlow(totalUp);
        outV.setDownFlow(totalDown);
        outV.setSumFlow();

        //3.context写出
        context.write(key,outV);
    }
}

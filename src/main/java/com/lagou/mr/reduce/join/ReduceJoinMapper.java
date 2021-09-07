package com.lagou.mr.reduce.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, DeliverBean> {
    private String name = "";
    private DeliverBean bean = new DeliverBean();
    private Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        name = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("\t");
        if (name.startsWith("deliver_info")) {
            bean.setUserId(splits[0]);
            bean.setPositionId(splits[1]);
            bean.setDate(splits[2]);
            bean.setPositionName("");
            bean.setFlag("deliver_info");
        } else {
            bean.setPositionId(splits[0]);
            bean.setPositionName(splits[1]);
            bean.setUserId("");
            bean.setDate("");
            bean.setFlag("positiion");
        }
        k.set(bean.getPositionId());
        context.write(k,bean);
    }
}

package com.lagou.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text,SpeakBean, NullWritable> {
    final SpeakBean speakBean = new SpeakBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        speakBean.setDeviceId(fields[0]);
        speakBean.setSelfDuration(Long.parseLong(fields[1]));
        speakBean.setThirdPartDuration(Long.parseLong(fields[2]));
        speakBean.setSumDuration(Long.parseLong(fields[4]));
        context.write(speakBean,NullWritable.get());
    }
}

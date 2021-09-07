package com.lagou.speaker;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpeakerMapper extends Mapper<LongWritable, Text,Text,SpeakBean> {
    final Text text = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] speakerInfo = value.toString().split("\t");
        String deviceId = speakerInfo[1];
        String selfDuration = speakerInfo[speakerInfo.length-3];
        String thirdDuration = speakerInfo[speakerInfo.length-2];
        SpeakBean speakBean = new SpeakBean(Long.parseLong(selfDuration),Long.parseLong(thirdDuration),deviceId);
        text.set(deviceId);
        context.write(text, speakBean);
    }
}

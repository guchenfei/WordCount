package com.lagou.speaker;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SpeakReducer extends Reducer<Text,SpeakBean,Text,SpeakBean> {
    @Override
    protected void reduce(Text key, Iterable<SpeakBean> values, Context context) throws IOException, InterruptedException {
        Long sumSelfDuration = 0L;
        Long sumThirdDuration = 0L;
        for (SpeakBean speakBean : values) {
            sumSelfDuration+=speakBean.getSelfDuration();
            sumThirdDuration+=speakBean.getThirdPartDuration();
        }
        final SpeakBean speakBean = new SpeakBean(sumSelfDuration,sumThirdDuration,key.toString());
        context.write(key,speakBean);
    }
}

package com.lagou.sequence;

import com.lagou.wc.WordCombiner;
import com.lagou.wc.WordCountDriver;
import com.lagou.wc.WordCountMapper;
import com.lagou.wc.WordCountReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SequenceDriver {
    public static void main(String[] args) {
        try {
            Configuration con = new Configuration();
            Job job = Job.getInstance(con, "SequenceDriver");
            job.setJarByClass(SequenceDriver.class);
            job.setMapperClass(SequenceMapper.class);
            job.setReducerClass(SequenceReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(BytesWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(BytesWritable.class);
            job.setInputFormatClass(CustomInputFormat.class);
            FileInputFormat.setInputPaths(job, new Path("/Users/guchenfei/Downloads/input2/"));
            FileOutputFormat.setOutputPath(job, new Path("/Users/guchenfei/Downloads/output"));
            boolean flag = job.waitForCompletion(true);
            System.exit(flag ? 0 : 1);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {

        }
    }
}

package com.lagou.mr.reduce.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ReduceJoinDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration con = new Configuration();
        Job job = Job.getInstance(con, "ReduceJoinDriver");
        job.setJarByClass(ReduceJoinDriver.class);
        job.setMapperClass(ReduceJoinMapper.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DeliverBean.class);
        job.setOutputKeyClass(DeliverBean.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path("/Users/guchenfei/Downloads/input2"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/guchenfei/Downloads/output"));
        boolean flag = job.waitForCompletion(true);
        System.exit(flag ? 0 : 1);
    }
}

package com.lagou.mr.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OutputDriver {
    public static void main(String[] args) {
        try {
            Configuration con = new Configuration();
            Job job = Job.getInstance(con, "OutputDriver");
            job.setJarByClass(OutputDriver.class);
            job.setMapperClass(OutputMapper.class);
            job.setReducerClass(OutputReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            job.setOutputFormatClass(CustomOutputFormat.class);
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

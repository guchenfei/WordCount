package com.lagou.comment.step1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class CombineDriver {
    public static void main(String[] args) {
        try {
            Configuration con = new Configuration();
            Job job = Job.getInstance(con, "CombineDriver");
            job.setJarByClass(CombineDriver.class);
            job.setMapperClass(CombineMapper.class);
            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            job.setInputFormatClass(CombineTextInputFormat.class);
            job.setNumReduceTasks(3);
            CombineTextInputFormat.setMaxInputSplitSize(job, 1024 * 1024 * 4);
            FileInputFormat.setInputPaths(job, new Path("/Users/guchenfei/Downloads/input2/"));
            FileOutputFormat.setOutputPath(job, new Path("/Users/guchenfei/Downloads/output-combine"));
            boolean flag = job.waitForCompletion(true);
            System.exit(flag ? 0 : 1);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {

        }
    }
}

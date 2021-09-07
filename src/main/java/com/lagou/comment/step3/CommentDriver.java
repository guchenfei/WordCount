package com.lagou.comment.step3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CommentDriver {
    public static void main(String[] args) {
        try {
            Configuration con = new Configuration();
            Job job = Job.getInstance(con, "CommentDriver");
            job.setJarByClass(CommentDriver.class);
            job.setMapperClass(CommentMapper.class);
            job.setReducerClass(CommentReducer.class);
            job.setMapOutputKeyClass(CommentBean.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setOutputKeyClass(CommentBean.class);
            job.setOutputValueClass(NullWritable.class);
            job.setPartitionerClass(CommentPartitioner.class);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(CommentOutputFormat.class);
            job.setNumReduceTasks(3);
//            job.setGroupingComparatorClass(CommentGroupComparator.class);
            FileInputFormat.setInputPaths(job, new Path("/Users/guchenfei/Downloads/output"));
            FileOutputFormat.setOutputPath(job, new Path("/Users/guchenfei/Downloads/output-muti"));
            boolean flag = job.waitForCompletion(true);
            System.exit(flag ? 0 : 1);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {

        }
    }
}

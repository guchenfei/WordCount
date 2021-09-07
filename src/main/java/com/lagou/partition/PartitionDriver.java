package com.lagou.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class PartitionDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration con = new Configuration();
        Job job = Job.getInstance(con);
        job.setJarByClass(PartitionDriver.class);
        job.setMapperClass(PartitionMapper.class);
        job.setReducerClass(PartitionReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PartitionBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PartitionBean.class);
        job.setPartitionerClass(CustomPartitioner.class);
        job.setNumReduceTasks(3);
        FileInputFormat.setInputPaths(job,new Path("/Users/guchenfei/Downloads/input/speak.txt"));
        FileOutputFormat.setOutputPath(job,new Path("/Users/guchenfei/Downloads/output"));
        boolean flag = job.waitForCompletion(true);
        System.exit(flag?0:1);
    }
}

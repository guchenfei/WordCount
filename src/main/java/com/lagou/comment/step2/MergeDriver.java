package com.lagou.comment.step2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class MergeDriver {
    public static void main(String[] args) {
        try {
            Configuration con = new Configuration();
            Job job = Job.getInstance(con, "MergeDriver");
            job.setJarByClass(MergeDriver.class);
            job.setMapperClass(MergeMapper.class);
            job.setReducerClass(MergeReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(BytesWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(BytesWritable.class);
            job.setInputFormatClass(MergeInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.RECORD);
            FileInputFormat.setInputPaths(job, new Path("/Users/guchenfei/Downloads/output-combine"));
            FileOutputFormat.setOutputPath(job, new Path("/Users/guchenfei/Downloads/output"));
            boolean flag = job.waitForCompletion(true);
            System.exit(flag ? 0 : 1);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {

        }
    }
}

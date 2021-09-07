package com.lagou.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) {
        try {
            Configuration con = new Configuration();
            con.set("mapreduce.output.fileoutputformat.compress","true");
            con.set("mapreduce.output.fileoutputformat.compress.type","RECORD");
            con.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
            Job job = Job.getInstance(con, "WordCountDriver");
            job.setJarByClass(WordCountDriver.class);
            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordCountReduce.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setCombinerClass(WordCombiner.class);
            FileInputFormat.setInputPaths(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job,new Path(args[1]));
            boolean flag = job.waitForCompletion(true);
            System.exit(flag?0:1);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {

        }
    }
}

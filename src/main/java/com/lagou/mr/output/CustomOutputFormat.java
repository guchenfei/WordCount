package com.lagou.mr.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CustomOutputFormat extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        FileSystem fileSystem = FileSystem.get(conf);
        FSDataOutputStream lagouOut = fileSystem.create(new Path("/Users/guchenfei/Downloads/aaa/lagou.txt"));
        FSDataOutputStream otherOut = fileSystem.create(new Path("/Users/guchenfei/Downloads/bbb/other.txt"));
        CustomWriter customWriter = new CustomWriter(lagouOut,otherOut);
        return customWriter;
    }
}

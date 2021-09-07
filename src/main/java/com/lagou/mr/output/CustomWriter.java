package com.lagou.mr.output;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class CustomWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream lagouOut;
    private FSDataOutputStream otherOut;

    public CustomWriter(FSDataOutputStream lagouOut, FSDataOutputStream otherOut) {
        this.lagouOut = lagouOut;
        this.otherOut = otherOut;
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String line = key.toString();
        if (line.contains("lagou")){
            lagouOut.write(line.getBytes());
            lagouOut.write("\r\n".getBytes());
        }else {
            otherOut.write(line.getBytes());
            otherOut.write("\r\n".getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(otherOut);
        IOUtils.closeStream(lagouOut);
    }
}

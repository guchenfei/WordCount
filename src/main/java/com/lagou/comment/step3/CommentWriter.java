package com.lagou.comment.step3;


import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class CommentWriter extends RecordWriter<CommentBean, NullWritable> {
    private FSDataOutputStream goodOut;
    private FSDataOutputStream commonOut;
    private FSDataOutputStream badOut;

    public CommentWriter(FSDataOutputStream goodOut, FSDataOutputStream commonOut, FSDataOutputStream badOut) {
        this.goodOut = goodOut;
        this.commonOut = commonOut;
        this.badOut = badOut;
    }

    @Override
    public void write(CommentBean key, NullWritable value) throws IOException, InterruptedException {
        int commentStatus = key.getCommentStatus();
        switch (commentStatus){
            case 0:
                goodOut.write(key.toString().getBytes());
                goodOut.write("\r\n".getBytes());
                goodOut.flush();
                break;
            case 1:
                commonOut.write(key.toString().getBytes());
                commonOut.write("\r\n".getBytes());
                commonOut.flush();
                break;
            default:
                badOut.write(key.toString().getBytes());
                badOut.write("\r\n".getBytes());
                badOut.flush();
                break;
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(goodOut);
        IOUtils.closeStream(commonOut);
        IOUtils.closeStream(badOut);
    }
}

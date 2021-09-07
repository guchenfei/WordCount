package com.lagou.comment.step3;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CommentOutputFormat extends FileOutputFormat<CommentBean, NullWritable> {
   private FSDataOutputStream goodOut = null;
   private FSDataOutputStream commonOut = null;
   private FSDataOutputStream badOut = null;

    @Override
    public RecordWriter<CommentBean, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        int id = job.getTaskAttemptID().getTaskID().getId();
        Configuration conf = job.getConfiguration();
        String outDir = conf.get("mapreduce.output.fileoutputformat.outputdir");
        FileSystem fileSystem = FileSystem.get(conf);
        switch (id){
            case 0:
                goodOut = fileSystem.create(new Path(outDir + "/good/good.log"));
                break;
            case 1:
                commonOut = fileSystem.create(new Path(outDir + "/common/common.log"));
                break;
            default:
                badOut = fileSystem.create(new Path(outDir + "/bad/bad.log"));
                break;
        }
        CommentWriter commentWriter = new CommentWriter(goodOut, commonOut,badOut);
        return commentWriter;
    }
}

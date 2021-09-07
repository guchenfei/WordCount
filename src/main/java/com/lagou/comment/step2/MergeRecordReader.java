package com.lagou.comment.step2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


public class MergeRecordReader extends RecordReader<Text, BytesWritable> {
    private FileSplit mSplit;
    private Configuration conf;
    private Text fileName = new Text();
    private BytesWritable fileContent = new BytesWritable();
    private boolean flag = true;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        mSplit = (FileSplit) split;
        conf = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (flag) {
            byte[] content = new byte[(int) mSplit.getLength()];
            Path path = mSplit.getPath();
            FileSystem fileSystem = path.getFileSystem(conf);
            FSDataInputStream fis = fileSystem.open(path);
            IOUtils.readFully(fis, content, 0, content.length);
            fileName.set(path.toString());
            fileContent.set(content, 0, content.length);
            IOUtils.closeStream(fis);
            flag = false;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return fileName;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return fileContent;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}

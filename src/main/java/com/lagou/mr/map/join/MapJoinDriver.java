package com.lagou.mr.map.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapJoinDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Configuration con = new Configuration();
        Job job = Job.getInstance(con, "MapJoinDriver");
        job.setJarByClass(MapJoinDriver.class);
        job.setMapperClass(MapJoinMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.addCacheFile(new URI("file:///Users/guchenfei/Downloads/input2/input/position.txt"));
        job.setNumReduceTasks(0);
        FileInputFormat.setInputPaths(job, new Path("/Users/guchenfei/Downloads/input2/deliver_info.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/guchenfei/Downloads/output"));
        boolean flag = job.waitForCompletion(true);
        System.exit(flag ? 0 : 1);
    }
}

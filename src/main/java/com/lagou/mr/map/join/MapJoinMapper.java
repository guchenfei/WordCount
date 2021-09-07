package com.lagou.mr.map.join;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    Map<String,String> positionMap = new HashMap<>();
    Text info = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream("position.txt"), "UTF-8");
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String line;
        while (StringUtils.isNotEmpty(line = bufferedReader.readLine())){
            String[] splits = line.split("\t");
            positionMap.put(splits[0],splits[1]);
        }
        bufferedReader.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("\t");
        String line = value.toString() + "\t" + positionMap.get(splits[1]);
        info.set(line);
        context.write(info,NullWritable.get());
    }
}

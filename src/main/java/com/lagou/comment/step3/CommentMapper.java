package com.lagou.comment.step3;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CommentMapper extends Mapper<Text, BytesWritable, CommentBean, NullWritable> {
    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        String s = new String(value.getBytes());
        String[] lines = s.split("\\n");
        for (String line : lines) {
            CommentBean commentBean = parseStrToCommentBean(line);
            if (commentBean != null){
                context.write(commentBean, NullWritable.get());
            }
        }
    }

    private CommentBean parseStrToCommentBean(String line) {
        if (StringUtils.isNotBlank(line)) {
            String[] splits = line.split("\t");
            if (splits.length >= 9) {
                return new CommentBean(splits[0], splits[1], splits[2], Integer.parseInt(splits[3]), splits[4], splits[5], splits[6], Integer.parseInt(splits[7]), splits[8]);
            }
        }
        return null;
    }
}

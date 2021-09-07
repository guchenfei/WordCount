package com.lagou.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<Text,PartitionBean> {

    @Override
    public int getPartition(Text text, PartitionBean partitionBean, int numPartitions) {
        int partition;
        if (text.toString().equals("kar")){
            partition = 0;
        }else if (text.toString().equals("pandora")){
            partition = 1;
        }else {
            partition = 2;
        }
        return partition;
    }
}

package com.lagou.comment.step3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CommentGroupComparator extends WritableComparator {
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CommentBean a1 = (CommentBean) a;
        CommentBean b1 = (CommentBean) b;
        return a1.getOrderId().compareTo(b1.getOrderId());
    }
}

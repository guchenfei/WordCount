package com.lagou.mr.reduce.join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class ReduceJoinReducer extends Reducer<Text, DeliverBean, DeliverBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<DeliverBean> values, Context context) throws IOException, InterruptedException {
        List<DeliverBean> deliverList = new ArrayList<>();
        DeliverBean positionBean = new DeliverBean();
        for (DeliverBean bean : values) {
            if (bean.getFlag().equalsIgnoreCase("deliver_info")) {
                DeliverBean deliverBean = new DeliverBean();
                try {
                    BeanUtils.copyProperties(deliverBean, bean);
                    deliverList.add(deliverBean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    BeanUtils.copyProperties(positionBean,bean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        for (DeliverBean deliverBean : deliverList) {
            deliverBean.setPositionName(positionBean.getPositionName());
            context.write(deliverBean,NullWritable.get());
        }
    }
}

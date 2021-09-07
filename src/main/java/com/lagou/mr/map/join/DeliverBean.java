package com.lagou.mr.map.join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DeliverBean implements Writable {
    private String userId;
    private String positionId;
    private String date;
    private String positionName;
    private String flag;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(userId);
        dataOutput.writeUTF(positionId);
        dataOutput.writeUTF(date);
        dataOutput.writeUTF(positionName);
        dataOutput.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.userId = dataInput.readUTF();
        this.positionId = dataInput.readUTF();
        this.date = dataInput.readUTF();
        this.positionName = dataInput.readUTF();
        this.flag = dataInput.readUTF();
    }

    public DeliverBean() {
    }

    public DeliverBean(String userId, String positionId, String date, String positionName, String flag) {
        this.userId = userId;
        this.positionId = positionId;
        this.date = date;
        this.positionName = positionName;
        this.flag = flag;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getPositionId() {
        return positionId;
    }

    public void setPositionId(String positionId) {
        this.positionId = positionId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getPositionName() {
        return positionName;
    }

    public void setPositionName(String positionName) {
        this.positionName = positionName;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return "DeliverBean{" +
                "userId='" + userId + '\'' +
                ", positionId='" + positionId + '\'' +
                ", date='" + date + '\'' +
                ", positionName='" + positionName + '\'' +
                ", flag='" + flag + '\'' +
                '}';
    }
}

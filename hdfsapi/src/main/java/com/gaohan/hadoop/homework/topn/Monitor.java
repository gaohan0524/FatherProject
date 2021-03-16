package com.gaohan.hadoop.homework.topn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * monitor类 实现WritableComparable<>接口
 */
public class Monitor implements WritableComparable<Monitor> {

    private String thing;
    private int num;

    public Monitor() {
        super();
    }

    public Monitor(String thing, int num) {
        this.thing = thing;
        this.num = num;
    }

    @Override
    public int compareTo(Monitor monitor) {
        return -(Integer.compare(this.num, monitor.num));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(thing);
        out.writeInt(num);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.thing = in.readUTF();
        this.num = in.readInt();
    }

    @Override
    public String toString() {
        return "monitor{" +
                "thing='" + thing + '\'' +
                ", num=" + num +
                '}';
    }

    public String getThing() {
        return thing;
    }

    public void setThing(String thing) {
        this.thing = thing;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }
}

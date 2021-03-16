package com.gaohan.hadoop.mr.ser;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义MR中的序列化对象 access
 * 1）实现Writable接口
 * 2）重写write方法和readFields方法，注意：顺序和类型
 * 3）建议重写toString方法
 * 4）手工加上一个无参构造器
 *
 */
public class Access implements Writable {


    private String phone;
    private long up;
    private long down;
    private long sum;

    // 无参构造器
    public Access() {

    }

    // 序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phone);
        out.writeLong(up);
        out.writeLong(down);
        out.writeLong(sum);
    }

    // 反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        // 反序列化和序列化方法的字段操作顺序一定要一致
        phone = in.readUTF();
        up = in.readLong();
        down = in.readLong();
        sum = in.readLong();
    }

    // 重写toString方法
    @Override
    public String toString() {
        return "Access{" +
                "phone='" + phone + '\'' +
                ", up=" + up +
                ", down=" + down +
                ", sum=" + sum +
                '}';
    }


    // 4个参数的getter和setter方法
    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public long getUp() {
        return up;
    }

    public void setUp(long up) {
        this.up = up;
    }

    public long getDown() {
        return down;
    }

    public void setDown(long down) {
        this.down = down;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }
}

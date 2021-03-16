package com.gaohan.hadoop.homework.grouptopn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Hobby类 实现WritableComparable<>接口
 */
public class Hobby implements WritableComparable<Hobby> {

    private Integer id;
    private String thing;
    private Double price;

    public Hobby() {

    }

    public Hobby(Integer id, String thing, Double price) {
        this.id = id;
        this.thing = thing;
        this.price = price;
    }


    @Override
    public int compareTo(Hobby hobby) {
        int result = this.id.compareTo(hobby.id);
        if (result == 0) {
            result = -this.price.compareTo(hobby.price);
        }
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(thing);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.thing = in.readUTF();
        this.price = in.readDouble();
    }

    @Override
    public String toString() {
        return "Hobby{" +
                "id=" + id +
                ", price=" + price +
                '}';
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getThing() {
        return thing;
    }

    public void setThing(String thing) {
        this.thing = thing;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }
}

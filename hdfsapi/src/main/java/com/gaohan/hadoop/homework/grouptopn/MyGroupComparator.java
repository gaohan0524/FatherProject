package com.gaohan.hadoop.homework.grouptopn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupComparator extends WritableComparator {

    public MyGroupComparator() {
        super(Hobby.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Hobby hobbyA = (Hobby) a;
        Hobby hobbyB = (Hobby) b;
        return hobbyA.getId().compareTo(hobbyB.getId());
    }
}

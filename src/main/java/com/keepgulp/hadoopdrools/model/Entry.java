package com.keepgulp.hadoopdrools.model;

public class Entry {
    public String key;
    public Double value;

    public Entry(String key, Double value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return "Entry [key=" + key + ", value=" + value + "]";
    }
}

package com.keepgulp.hadoopdrools.model;

public class RunningValue {
    public String key;
    public Double value;

    public RunningValue(String key, Double value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return "RunningReduce [key=" + key + ", value=" + value + "]";
    }
}

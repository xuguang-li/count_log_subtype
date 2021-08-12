package com.pmi.check_counter.offline_logs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {
    public static final Logger LOG = LoggerFactory.getLogger(Reducer.class);

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value: values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
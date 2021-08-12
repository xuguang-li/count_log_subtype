package com.pmi.check_counter.online_logs;

import net.sf.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import com.pmi.check_counter.online_logs.Maper;
import com.pmi.check_counter.online_logs.Reduce;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import com.hadoop.mapreduce.LzoTextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.*;
import net.sf.json.JSONArray;

import com.yidian.cpp.common.util.json.JsonUtil;
import com.yidian.cpp.common.exception.JsonDeserializeException;

public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        Job job = Job.getInstance(conf);
        job.setJarByClass(com.pmi.check_counter.online_logs.Main.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(Maper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        String[] remainingArgs = optionParser.getRemainingArgs();
        FileInputFormat.setInputPaths(job, remainingArgs[0]);
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));

        boolean ret = job.waitForCompletion(true);
        System.exit(ret ? 0 : 1);

    }
}

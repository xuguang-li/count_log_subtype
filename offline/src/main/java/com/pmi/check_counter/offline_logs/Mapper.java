package com.pmi.check_counter.offline_logs;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.io.IOException;
import java.util.*;

import com.yidian.cpp.common.util.json.JsonUtil;
import com.yidian.cpp.common.exception.JsonDeserializeException;


public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {

    public static final Logger LOG = LoggerFactory.getLogger(Mapper.class);

    private Text key_out = new Text();

    private IntWritable value_out = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {

        String json = value.toString();
        Map<String, Object> entry = new HashMap<String, Object>();
                try {
                    entry = JsonUtil.readAsMap(json);
                } catch (JsonDeserializeException ex) {
                    LOG.error("Invalid json %s: `%s`", ex, json);
                }

        if (entry == null) {
            return;
        }
        if (entry.get("subType") == null) {
            return;
        }
        if (entry.get("appid") != null) {
            if (!entry.get("appid").toString().equals("newsbreak")) {
                return;
            }
        }
        String os = entry.get("model").toString();
        String subtype = entry.get("subType").toString();

        if (subtype == null || subtype.isEmpty()) {
            LOG.error("Got empty line from subtype");
            return;
        }
        int num = 1;
        if (subtype.equals("changeChannel")){
            if (entry.get("context") != null) {
                Map<String, Object> context_c = JsonUtil.readAsMap(JsonUtil.toJson(entry.get("context")));
//                JSONObject context_c = JSONObject.fromObject(entry.get("context"));
                if (context_c.get("appid")!=null) {
                    if (!context_c.get("appid").toString().equals("newsbreak")) {
                        return;
                    }
                }
                if (context_c.get("checkedView") != null) {
                    num = 0;
                    try {

                        Collection<Object> elems = (Collection<Object>) context_c.get("checkedView");
                        for (Object elem : elems) {
                            JSONObject elem_json = JSONObject.fromObject(elem);
                            if (elem_json.get("docIds") != null) {
                                num += elem_json.getJSONArray("docIds").size();
                            }
                        }
                    }catch (Exception e){
                        LOG.error("Invalid json %s: `%s`",e,json);
                    }
                }
            }
        }


        
        //LOG.debug(json);
        key_out.set(os+"-"+subtype);
        value_out.set(num);
        context.write(key_out, value_out);
    }
}

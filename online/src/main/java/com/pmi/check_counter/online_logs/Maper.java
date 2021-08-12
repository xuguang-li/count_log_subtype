package com.pmi.check_counter.online_logs;

import com.google.inject.internal.util.$Strings;
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


public class Maper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {

    public static final Logger LOG = LoggerFactory.getLogger(com.pmi.check_counter.online_logs.Maper.class);

    private Text key_out = new Text();

    private IntWritable value_out = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {

        String json = value.toString();
        Map<String, Object> entry_log = new HashMap<String, Object>();
        try {
            entry_log = JsonUtil.readAsMap(json);
        } catch (JsonDeserializeException ex) {
            LOG.error("Invalid json %s: `%s`", ex, json);
        }
        if (entry_log.get("log") == null) {
            return;
        }
//        try {
//            Map<String, Object> entry = JsonUtil.readAsMap(entry_log.get("log").toString());
//        } catch (Exception ex) {
//            System.out.println(entry_log);
//            LOG.error("Invalid json %s: `%s`", ex, entry_log);
//        }
        Map<String, Object> entry = JsonUtil.readAsMap(JsonUtil.toJson(entry_log.get("log")));
        if (entry.get("subType") == null) {
            return;
        }
        String os = "";
        if (entry_log.get("platform")!=null){
            os = entry_log.get("platform").toString();
        }
        if (entry.get("platform")!=null){
            os = entry.get("platform").toString();
        }
        if (os.equals("1")){
            os = "android";
        }else if(os.equals("0")) {
            os = "ios";
        }else {
//            os = "unknown";
            return;
        }

        if (entry.get("appid") != null) {
            if (!entry.get("appid").toString().equals("newsbreak")) {
                return;
            }
        }
        if (entry_log.get("appid") != null) {
            if (!entry_log.get("appid").toString().equals("newsbreak")) {
                return;
            }
        }

        String subtype = entry.get("subType").toString();

        if (subtype == null || subtype.isEmpty()) {
            LOG.error("Got empty line from subtype");
            return;
        }
        int num = 1;
        if (subtype.equals("changeChannel")){
            num = 0;
            if (entry.get("checkedView") != null) {
                try {

                    Collection<Object> elems = (Collection<Object>) entry.get("checkedView");
                    for (Object elem : elems) {
//                        JSONObject elem_json = JSONObject.fromObject(elem);
                        Map<String, Object> elem_json = JsonUtil.readAsMap(JsonUtil.toJson(elem));
                        if (elem_json.get("docIds") != null) {
                            Collection<String> docs = (Collection<String>) elem_json.get("docIds");
                            num += docs.size();
                        }
                    }
                }catch (Exception e){
                    LOG.error("Invalid json %s: `%s`",e,json);
                }

            }
        }
        //LOG.debug(json);
        key_out.set(os+"-"+subtype);
        value_out.set(num);
        context.write(key_out, value_out);
    }
}

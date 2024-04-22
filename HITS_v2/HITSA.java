package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class HITSA {
    public static class MyMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] kv = String.valueOf(value).split("\t");
            String key_node = kv[0];
            String[] values = kv[1].split("\\|");
            String nodes = values[0];
            String auth = values[1];
            String hub = values[2];
            String[] arr_nodes = nodes.split(":");
            if(arr_nodes.length>1) {
                String[] arr_node = arr_nodes[1].split(",");
                for (String node : arr_node) {
                    context.write(new Text(node), new Text(key_node+"|"+hub));
                }
            }
            context.write(new Text(key_node), new Text(auth+"|"+hub));
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double auth=0;
            double hub=0;
            String nodes = "nodes:";
            for (Text v : values){
                String[] one = v.toString().split("\\|");
                String[] two = one[0].split(":");
                if(two.length==1){
                    nodes+=two[0]+",";
                    two = one[1].split(":");
                    auth+=Double.valueOf(two[1]);
                }else{
                    two=one[1].split(":");
                    hub=Double.valueOf(two[1]);
                }
            }
            context.write(new Text(key),new Text(nodes+"|auth:"+auth+"|hub:"+hub));
        }
    }
}

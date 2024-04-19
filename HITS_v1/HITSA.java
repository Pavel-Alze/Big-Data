package org.example;

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
            String[] k_v = String.valueOf(value).split("\t");
            String[] k = k_v[0].split(":");
            if(k_v.length==1){
                context.write(new Text(k[0]),new Text("-1"));
            }else {
                String[] values = k_v[1].split(",");
                for (String s : values){
                    context.write(new Text(s),new Text(k_v[0]));
                }
                context.write(new Text(k[0]),new Text("-1"));
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double auth=0;
            String res="";
            for (Text t : values){
                if (!String.valueOf(t).equals("-1")){
                    try{
                        String[] val = String.valueOf(t).split(":");
                        auth += Double.valueOf(val[1]);
                        res+=val[0]+",";
                    }catch (Exception e){}
                }
            }
            context.write(new Text(String.valueOf(key)+":"+auth),new Text(res));
        }
    }
}

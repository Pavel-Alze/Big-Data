package org.example;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author joker
 */
public class CrossCorelation2 {
    public static class MyMapper extends Mapper<Object, Text, Text, MapWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final Text wordsMapping = new Text();
        private ArrayList<Text> products;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            products = new ArrayList<Text>();

            StringTokenizer st = new StringTokenizer(value.toString(), ",");

            while (st.hasMoreTokens()) {
                Text word = new Text();
                word.set(st.nextToken());
                products.add(word);
            }

            //System.out.println(Arrays.asList(products));

            for (Text word1 : products) {
                Map<Text, IntWritable> tmpData = new HashMap<>();
                for (Text word2 : products) {
                    if (!word1.equals(word2)) {
                        if (!tmpData.containsKey(word2)) {
                            tmpData.put(word2, one);
                        }
                    }
                }
                System.out.println(tmpData.entrySet());
                MapWritable resultMap = new MapWritable();
                for (Map.Entry<Text, IntWritable> entryTmpData : tmpData.entrySet()) {
                    resultMap.put(entryTmpData.getKey(), entryTmpData.getValue());
                }
                context.write(word1, resultMap);
            }


        }
    }

    public static class MyReducer extends Reducer<Text, MapWritable, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {

            MapWritable resultData = new MapWritable();
            Map<Text,Integer> tmp = new HashMap<>();
            System.out.println(key);
            for (MapWritable tmpData : values) {
                System.out.println(tmpData.entrySet());
                if (tmp.isEmpty()){
                    for (Map.Entry<Writable, Writable> entryTmpData : tmpData.entrySet()) {
                        tmp.put((Text) entryTmpData.getKey(), 1);
                    }
                }else{
                    for (Writable key1: tmpData.keySet()){
                        if(tmp.containsKey(key1)){
                            tmp.replace((Text) key1,tmp.get(key1)+1);
                        }else {
                            tmp.put((Text) key1,1);
                        }
                    }
                }
                /*for (Map.Entry<Writable, Writable> entryData : tmpData.entrySet()) {
                    resultData.merge(new Text(key + " " + entryData.getKey().toString()), entryData.getValue(),
                            (oldValue, addValue) -> new IntWritable(((IntWritable) oldValue).get() + ((IntWritable) addValue).get()));
                }*/
            }
            Text res = new Text("");
            for(Text key2 : tmp.keySet()){
                    res = new Text(res + ";" + key2.toString() + ":" + tmp.get(key2).toString());
            }
            context.write(key,res);
            /*for (Map.Entry<Writable, Writable> outputData : resultData.entrySet()) {
                context.write((WritableComparable) outputData.getKey(), (IntWritable) outputData.getValue());
            }*/
        }
    }
}
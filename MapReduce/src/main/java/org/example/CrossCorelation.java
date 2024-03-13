package org.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import javafx.util.Pair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author joker
 */
public class CrossCorelation {

    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private ArrayList<Text> products;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            products = new ArrayList<>();

            StringTokenizer st = new StringTokenizer(value.toString(), ",");

            while (st.hasMoreTokens()) {
                Text word = new Text();
                word.set(st.nextToken());
                products.add(word);
            }

            for(int i = 0; i < products.size(); ++i)
                for(int j = 0; j < products.size(); ++j)
                    if (products.get(i) != products.get(j)) {
                        context.write(new Text(products.get(i).toString() + "," + products.get(j).toString()), one);
                    }
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            IntWritable result = new IntWritable();
            System.out.println(key);
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            System.out.println(result.toString());
            context.write(key, result);
        }
    }
}

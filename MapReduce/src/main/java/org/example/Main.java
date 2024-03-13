package org.example;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
import java.io.IOException;
import java.util.AbstractMap;
import javafx.util.Pair;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
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
public class Main {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "cross_corelation");
        job.setJarByClass(Main.class);
        boolean b = false;
        if(b) {
            setConfForPairs(job);
        }else {
            setConfForStripes(job);
        }
        job.waitForCompletion(true);
        if(b) {
            Rec rec = new Rec();
            rec.rec();
        }else {
            Rec2 rec2 = new Rec2();
            rec2.rec();
        }
    }
    private static void setConfForStripes(Job job) throws IOException {
        job.setMapperClass(CrossCorelation2.MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setReducerClass(CrossCorelation2.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/root/BigData/mysources/input.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/root/BigData/mysources/output2"));
    }

    private static void setConfForPairs(Job job) throws IOException {
        job.setMapperClass(CrossCorelation.MyMapper.class);
        job.setCombinerClass(CrossCorelation.MyReducer.class);
        job.setReducerClass(CrossCorelation.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("/root/BigData/mysources/input.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/root/BigData/mysources/output"));
    }

}
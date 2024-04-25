package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "HITSA");
        job.setJarByClass(Main.class);
        job.setMapperClass(HITSA.MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(HITSA.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/root/BigData/mysources/input.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/root/BigData/mysources/ath_out"));
        if(job.waitForCompletion(true)) {
            job = new Job(conf, "NormA");
            job.setJarByClass(Main.class);
            job.setMapperClass(Normalized.CountMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setReducerClass(Normalized.CountReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("/root/BigData/mysources/ath_out/part-r-00000"));
            FileOutputFormat.setOutputPath(job, new Path("/root/BigData/mysources/norma_out"));
            if(job.waitForCompletion(true)) {
                job = new Job(conf, "JoinA");
                job.setJarByClass(Main.class);
                job.setMapperClass(Normalized.JoinAMapper.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job, new Path("/root/BigData/mysources/ath_out/part-r-00000"));
                FileOutputFormat.setOutputPath(job, new Path("/root/BigData/mysources/joina_out"));
                if(job.waitForCompletion(true)){
                    job = new Job(conf, "HITSH");
                    job.setJarByClass(Main.class);
                    job.setMapperClass(HITSH.MyMapper.class);
                    job.setMapOutputKeyClass(Text.class);
                    job.setMapOutputValueClass(Text.class);
                    job.setReducerClass(HITSH.MyReducer.class);
                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(Text.class);
                    FileInputFormat.addInputPath(job, new Path("/root/BigData/mysources/joina_out/part-r-00000"));
                    FileOutputFormat.setOutputPath(job, new Path("/root/BigData/mysources/hub_out"));
                    if(job.waitForCompletion(true)){
                        job = new Job(conf, "NormH");
                        job.setJarByClass(Main.class);
                        job.setMapperClass(Normalized.CountMapper.class);
                        job.setMapOutputKeyClass(Text.class);
                        job.setMapOutputValueClass(Text.class);
                        job.setReducerClass(Normalized.CountReducer.class);
                        job.setOutputKeyClass(Text.class);
                        job.setOutputValueClass(Text.class);
                        FileInputFormat.addInputPath(job, new Path("/root/BigData/mysources/hub_out/part-r-00000"));
                        FileOutputFormat.setOutputPath(job, new Path("/root/BigData/mysources/normh_out"));
                        if(job.waitForCompletion(true)){
                            job = new Job(conf, "JoinH");
                            job.setJarByClass(Main.class);
                            job.setMapperClass(Normalized.JoinHMapper.class);
                            job.setOutputKeyClass(Text.class);
                            job.setOutputValueClass(Text.class);
                            FileInputFormat.addInputPath(job, new Path("/root/BigData/mysources/hub_out/part-r-00000"));
                            FileOutputFormat.setOutputPath(job, new Path("/root/BigData/mysources/joinh_out"));
                            System.exit(job.waitForCompletion(true) ? 0 : 1);
                        }
                    }
                }
            }
        }
    }
}

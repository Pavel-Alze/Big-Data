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
        # входной файл смотреть `input`
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
        # запуск программы по обновлению авторитетности
        # выходной файл смотреть `ath`
        if(job.waitForCompletion(true)) {
            job = new Job(conf, "NormA");
            job.setJarByClass(Main.class);
            job.setMapperClass(Normalized.MyMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setReducerClass(Normalized.MyReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("/root/BigData/mysources/ath_out/part-r-00000"));
            FileOutputFormat.setOutputPath(job, new Path("/root/BigData/mysources/norma_out"));
            # запуск нормализации
            # выходной файл смотреть `norma`
            if(job.waitForCompletion(true)) {
                job = new Job(conf, "HITSH");
                job.setJarByClass(Main.class);
                job.setMapperClass(HITSH.MyMapper.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setReducerClass(HITSH.MyReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job, new Path("/root/BigData/mysources/norma_out/part-r-00000"));
                FileOutputFormat.setOutputPath(job, new Path("/root/BigData/mysources/hub_out"));
                # запуск программы по обновлению посредничества
                # выходной файл смотреть `hub`
                if(job.waitForCompletion(true)){
                    job = new Job(conf, "NormH");
                    job.setJarByClass(Main.class);
                    job.setMapperClass(Normalized.MyMapper.class);
                    job.setMapOutputKeyClass(Text.class);
                    job.setMapOutputValueClass(Text.class);
                    job.setReducerClass(Normalized.MyReducer.class);
                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(Text.class);
                    FileInputFormat.addInputPath(job, new Path("/root/BigData/mysources/hub_out/part-r-00000"));
                    FileOutputFormat.setOutputPath(job, new Path("/root/BigData/mysources/normh_out"));
                    # запуск нормализации
                    # выходной файл смотреть `normh`
                    System.exit(job.waitForCompletion(true) ? 0 : 1);
                }
            }
        }
    }
}

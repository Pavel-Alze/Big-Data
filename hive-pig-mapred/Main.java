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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author joker
 */
public class Main {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("car","vw");
        Job job = new Job(conf, "where");
        job.setJarByClass(Main.class);

        job.setMapperClass(WHERE.MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(WHERE.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/root/BigData/mysources/testpig2"));
        FileOutputFormat.setOutputPath(job, new Path("/root/BigData/mysources/whereout"));

        /*Job job = new Job(conf, "group");
        job.setJarByClass(Main.class);

        job.setMapperClass(GROUP.MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(GROUP.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("/root/BigData/mysources/testpig2"));
        FileOutputFormat.setOutputPath(job, new Path("/root/BigData/mysources/groupout"));*/

        /*Job job = new Job(conf, "JOIN");
        job.setJarByClass(Main.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(Join.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job,new Path("/root/BigData/mysources/testpig"), TextInputFormat.class,Join.JoinFirstMapper.class);
        MultipleInputs.addInputPath(job,new Path("/root/BigData/mysources/testpig1"), TextInputFormat.class,Join.JoinSecondMapper.class);
        FileOutputFormat.setOutputPath(job, new Path("/root/BigData/mysources/joinoutput"));*/


        job.waitForCompletion(true);



    }


}

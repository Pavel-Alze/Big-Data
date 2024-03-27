import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class GROUP {
    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private ArrayList<Text> products;

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        products = new ArrayList<Text>();
        StringTokenizer st = new StringTokenizer(value.toString(), ",");

        while (st.hasMoreTokens()) {
            Text word = new Text();
            word.set(st.nextToken());
            products.add(word);
        }
        context.write(products.get(1),one);
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
            context.write(key, result);
        }
    }
}

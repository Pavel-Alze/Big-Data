import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class WHERE {
    public static class MyMapper extends Mapper<Object, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private ArrayList<Text> products;

        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String car_value = conf.get("car");
            products = new ArrayList<Text>();
            StringTokenizer st = new StringTokenizer(value.toString(), ",");

            while (st.hasMoreTokens()) {
                Text word = new Text();
                word.set(st.nextToken());
                products.add(word);
            }
            if(products.get(1).toString().equals(car_value)) {
                context.write(products.get(1), products.get(0));
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text v : values){
                context.write(key,v);
            }
        }
    }
}

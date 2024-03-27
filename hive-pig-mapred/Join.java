import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

public class Join {
    public static class JoinFirstMapper extends Mapper<Object, Text, Text, Text> {
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
            context.write(products.get(1),products.get(0));
        }
    }

    public static class JoinSecondMapper extends Mapper<Object, Text, Text, Text> {
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
            context.write(products.get(0),products.get(1));
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        private ArrayList<String> products;
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            products = new ArrayList<String>();
            for (Text v : values){
                System.out.println(key+"#"+v);
                products.add(String.valueOf(v));
            }
            if(products.size()==1){
                try {
                    Integer.parseInt(products.get(0));
                    //System.out.println(t);
                    context.write(key,new Text(","+products.get(0)+",null"));
                }
                catch( Exception e ) {
                    context.write(key,new Text(",null,"+products.get(0)));
                }
            }else {
                System.out.println(Arrays.toString(products.toArray()));
                try {
                    Integer.parseInt(products.get(0));
                    //System.out.println(t);
                    context.write(key,new Text(","+products.get(0)+","+products.get(1)));
                }
                catch( Exception e ) {
                    context.write(key,new Text(","+products.get(1)+","+products.get(0)));
                }
            }
        }
    }
}

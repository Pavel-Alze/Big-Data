import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class Join2 {
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
            context.write(products.get(0), new Text(products.get(1).toString()+",1"));
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
            context.write(products.get(0),new Text(products.get(1).toString()+",2"));
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        private ArrayList<String> products;
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String res = "";
            String[] tmp = new String[]{"-1","-1"};
            for (Text v : values) {
                try {
                    String[] t = v.toString().split(",");
                    if(Integer.parseInt(t[1])==1) {
                        tmp[0]=t[0];
                    }
                    if(Integer.parseInt(t[1])==2) {
                        tmp[1]=t[0];
                    }
                }
                catch( Exception e ) {
                }
                System.out.println(key + "#" + v);
            }
            if(tmp[0].equals("-1") || tmp[1].equals("-1")){}else {
                context.write(key, new Text(","+tmp[0]+","+tmp[1]));
            }
        }
    }
}

package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class Normalized {
    public static class CountMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] kv = String.valueOf(value).split("\\|");
            String[] ath = kv[1].split(":");
            String[] hub = kv[2].split(":");
            context.write(new Text(ath[0]),new Text(ath[1]));
            context.write(new Text(hub[0]),new Text(hub[1]));

        }
    }

    public static class CountReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Double norm = 0.0;
            for (Text t : values){
                norm+=Double.valueOf(t.toString())*Double.valueOf(t.toString());
            }
            norm=Math.sqrt(norm);
            context.write(key,new Text(String.valueOf(norm)));
        }
    }


    public static class JoinAMapper extends Mapper<Object, Text, Text, Text> {

        private Map<String,Double> map = new HashMap<>();

        public void setup(Context context) throws IOException {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream inputStream = fs.open(new Path("/root/BigData/mysources/norma_out/part-r-00000"));
            byte[] bytes = new byte[(int) fs.getFileStatus(new Path("/root/BigData/mysources/norma_out/part-r-00000")).getLen()];
            inputStream.read(bytes);
            String localContent = new String(bytes, StandardCharsets.UTF_8);
            inputStream.close();
            fs.close();

            String[] lines = localContent.split("\n");
            String[] val = lines[0].split("\t");
            map.put(val[0].toString(),Double.valueOf(val[1].toString()));
            //val = lines[1].split("\t");
            //map.put(val[0].toString(),Double.valueOf(val[1].toString()));
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] kv = String.valueOf(value).split("\t");
            String key_node = kv[0];
            String[] values = kv[1].split("\\|");
            String nodes = values[0];
            String[] auth = values[1].split(":");
            String[] hub = values[2].split(":");

            auth[1] = String.valueOf(Double.valueOf(auth[1])/Double.valueOf(map.get(auth[0])));
            //hub[1] = String.valueOf(Double.valueOf(hub[1])/Double.valueOf(map.get(hub[0])));

            context.write(new Text(key_node),new Text(nodes+"|"+auth[0]+":"+auth[1]+"|"+hub[0]+":"+hub[1]));
        }
    }
    public static class JoinHMapper extends Mapper<Object, Text, Text, Text> {

        private Map<String,Double> map = new HashMap<>();

        public void setup(Context context) throws IOException {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream inputStream = fs.open(new Path("/root/BigData/mysources/normh_out/part-r-00000"));
            byte[] bytes = new byte[(int) fs.getFileStatus(new Path("/root/BigData/mysources/normh_out/part-r-00000")).getLen()];
            inputStream.read(bytes);
            String localContent = new String(bytes, StandardCharsets.UTF_8);
            inputStream.close();
            fs.close();
            String[] lines = localContent.split("\n");
            //String[] val = lines[0].split("\t");
            //map.put(val[0].toString(),Double.valueOf(val[1].toString()));
            String[] val = lines[1].split("\t");
            map.put(val[0].toString(),Double.valueOf(val[1].toString()));
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] kv = String.valueOf(value).split("\t");
            String key_node = kv[0];
            String[] values = kv[1].split("\\|");
            String nodes = values[0];
            String[] auth = values[1].split(":");
            String[] hub = values[2].split(":");

            //auth[1] = String.valueOf(Double.valueOf(auth[1])/Double.valueOf(map.get(auth[0])));
            hub[1] = String.valueOf(Double.valueOf(hub[1])/Double.valueOf(map.get(hub[0])));

            context.write(new Text(key_node),new Text(nodes+"|"+auth[0]+":"+auth[1]+"|"+hub[0]+":"+hub[1]));
        }
    }
}

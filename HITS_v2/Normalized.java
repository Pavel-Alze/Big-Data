package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class Normalized {
    public static class MyMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] kv = String.valueOf(value).split("\t");
            # отправляем из маппера всю строку, но с ключом "norm", чтобы на редьюсере все строки скомпоновались вместе
            context.write(new Text("norm"),new Text(kv[0]+"\t"+kv[1]));
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            # проводим нормализацию по формуле. делаем сразу для двух величин
            # если величина уже была нормализована, то она и не поменяется
            Double normA = 0.0;
            Double normH = 0.0;
            ArrayList<String> texts = new ArrayList<>();
            for(Text t: values){
                texts.add(t.toString());
                String[] temp = t.toString().split("\t");
                String[] temp1 = temp[1].split("\\|");
                temp = temp1[1].split(":");
                normA+=Double.valueOf(temp[1])*Double.valueOf(temp[1]);
                temp = temp1[2].split(":");
                normH+=Double.valueOf(temp[1])*Double.valueOf(temp[1]);
            }
            normA=Math.sqrt(normA);
            normH=Math.sqrt(normH);
            for(String s: texts){
                String[] kv = s.split("\t");
                String key_node=kv[0];
                String[] value = kv[1].split("\\|");
                String nodes = value[0];
                String[] auth = value[1].split(":");
                String[] hub = value[2].split(":");
                # возвращаем исходную строку, но с нормализованными величинами
                context.write(new Text(key_node),new Text(nodes+"|auth:"+Double.valueOf(auth[1])/normA+"|hub:"+Double.valueOf(hub[1])/normH));
            }
        }
    }
}

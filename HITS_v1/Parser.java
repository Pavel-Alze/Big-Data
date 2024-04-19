package org.example;

import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Parser {

    public void parse() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream inputStream = fs.open(new Path("/root/BigData/mysources/hub_out/part-r-00000"));
        byte[] bytes = new byte[(int) fs.getFileStatus(new Path("/root/BigData/mysources/hub_out/part-r-00000")).getLen()];
        inputStream.read(bytes);
        String localContent = new String(bytes, StandardCharsets.UTF_8);
        inputStream.close();
        String[] lines = localContent.split("\n");
        Map<String,Pair<Integer,Integer>> dict = new HashMap<>();
        List<String> values = new ArrayList<>();
        String res="";
        for (String l : lines){
            String[] k_v = l.split("\t");
            String[] key = k_v[0].split(":");
            dict.put(key[0],new Pair<>(Integer.parseInt(key[1]),0));
            if (!(k_v.length==1)) {
                values.add(k_v[1]);
            }
        }
        for (String s : values){
            String[] nodes = s.split(",");
            for(String n  : nodes) {
                String[] val = n.split(":");
                int auth = Integer.parseInt(val[1]);
                dict.replace(val[0], new Pair<>(dict.get(val[0]).getKey(), auth));
            }
        }
        for (String l : lines){
            String[] k_v = l.split("\t");
            String[] key = k_v[0].split(":");
            res+=key[0]+":"+dict.get(key[0]).getKey()+":"+dict.get(key[0]).getValue()+"\t";
            if (!(k_v.length==1)) {
                String[] nodes = k_v[1].split(",");
                for(String n  : nodes) {
                    String[] val = n.split(":");
                    res+=val[0]+",";
                }
            }
            res+="\n";
        }
        OutputStream out = fs.create(new Path("/root/BigData/mysources/input.txt"), true);
        out.write(res.getBytes(StandardCharsets.UTF_8));
        out.close();

        fs.delete(new Path("/root/BigData/mysources/.input.txt.crc"),true);
        fs.delete(new Path("/root/BigData/mysources/ath_out"),true);
        fs.delete(new Path("/root/BigData/mysources/hub_out"),true);
        fs.close();
    }
}

package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;

public class Rec2 {
    public void rec() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream inputStream = fs.open(new Path("/root/BigData/mysources/output2/part-r-00000"));
        byte[] bytes = new byte[(int) fs.getFileStatus(new Path("/root/BigData/mysources/output2/part-r-00000")).getLen()];
        inputStream.read(bytes);
        inputStream.close();
        String localContent = new String(bytes, StandardCharsets.UTF_8);
        System.out.println(localContent);
        String fin_result = "";
        String[] sp = localContent.split("\n");
        for(String s : sp){
            System.out.println("@@@@@@");
            System.out.println(s);
        }

        ArrayList<String[]> arr = new ArrayList<>();
        for(String s : sp) {
            arr.add(s.split(";"));
        }
        for (String[] spp : arr) {
            for (String ssp : spp) {
                System.out.print(ssp);
            }
            String result = "";
            LinkedHashMap<String, Integer> tr = new LinkedHashMap<>();
            result += spp[0];
            for (int i = 1; i < spp.length; i++) {
                String[] temp = spp[i].split(":");
                tr.put(temp[0], Integer.valueOf(temp[1]));
            }
            boolean b = true;
            int i = 0;
            System.out.println();
            while (b) {
                String[] keyset = tr.keySet().toArray(new String[0]);
                if (tr.get(keyset[i]) < tr.get(keyset[i + 1])) {
                    String temp_k = keyset[i];
                    int temp_v = tr.get(keyset[i]);
                    tr.remove(temp_k);
                    tr.put(temp_k, temp_v);
                    i=0;
                } else {
                    i++;
                }
                if (i == tr.size() - 1) {
                    b = false;
                }
                if (i == 10) {
                    b = false;
                }
            }
            System.out.println("-------------------");
            System.out.println(tr.entrySet());
            System.out.println("-------------------");
            String[] keys = tr.keySet().toArray(new String[0]);
            for (int ii = 0;;){
                while (ii<3 && ii < keys.length) {
                    result = result + " : " + keys[ii] + "=" + tr.get(keys[ii]);
                    ii++;
                }
                break;
            }
            fin_result+=result+"\n";
        }
        OutputStream out = fs.create(new Path("/root/BigData/mysources/output2/res"), true);
        out.write(fin_result.getBytes(StandardCharsets.UTF_8));
        out.close();
        fs.close();
    }
}

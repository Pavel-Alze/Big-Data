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

public class Rec {
    public void rec() throws IOException {
        Configuration conf = new Configuration();
        //TIP Press <shortcut actionId="ShowIntentionActions"/> with your caret at the highlighted text
        // to see how IntelliJ IDEA suggests fixing it.
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream inputStream = fs.open(new Path("/root/BigData/mysources/output/part-r-00000"));
        byte[] bytes = new byte[(int) fs.getFileStatus(new Path("/root/BigData/mysources/output/part-r-00000")).getLen()];
        inputStream.read(bytes);
        inputStream.close();
        String localContent = new String(bytes, StandardCharsets.UTF_8);
        System.out.println(localContent);
        String fin_result = "";
        System.out.println("---------");
        String[] sp = localContent.split("\n");
        LinkedHashMap<String, ArrayList<String>> k_v = new LinkedHashMap<>();
        for(String s : sp){
            System.out.println(s);
            String[] s1 = s.split(",");
            if(k_v.containsKey(s1[0])){
                ArrayList<String> te = k_v.get(s1[0]);
                te.add(s1[1]);
                k_v.replace(s1[0],te);
            }else {
                ArrayList<String> te = new ArrayList<>();
                te.add(s1[1]);
                k_v.put(s1[0], te);
            }
        }
        //System.out.println(k_v.entrySet());
        for (String key : k_v.keySet()) {
            String result = "";
            LinkedHashMap<String, Integer> tr = new LinkedHashMap<>();
            result += key;
            ArrayList<String> te = k_v.get(key);
            for (int i = 0; i < te.size(); i++) {
                String[] temp = te.get(i).split("\t");
                System.out.println(temp[0] + "==" + temp[1]);
                tr.put(temp[0], Integer.valueOf(temp[1]));
            }
            System.out.println(tr.entrySet());
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
                    i = 0;
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
            String[] keys = tr.keySet().toArray(new String[0]);
            for (int ii = 0; ; ) {
                while (ii < 3 && ii < keys.length) {
                    result = result + " : " + keys[ii] + "=" + tr.get(keys[ii]);
                    ii++;
                }
                break;
            }
            fin_result += result + "\n";
        }
        OutputStream out = fs.create(new Path("/root/BigData/mysources/output/res"), true);
        out.write(fin_result.getBytes(StandardCharsets.UTF_8));
        out.close();
        fs.close();
    }
}

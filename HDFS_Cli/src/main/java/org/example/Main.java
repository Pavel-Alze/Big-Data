package org.example;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Scanner;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws IOException {

        HDFS hdfs = new HDFS();
        int num = 1;
        while (num != 0){
            System.out.println("Enter action (1-11) or 0 ");
            System.out.println("1 - mkdir HDFS; 2 - mkdir Local");
            System.out.println("3 - upload to HDFS; 4 - download to Local; 5 - merge Local to HDFS");
            System.out.println("6 - del HDFS; 7 - del Local");
            System.out.println("8 - ls HDFS; 9 - ls Local");
            System.out.println("10 - cd HDFS; 11 - cd Local");
            Scanner in = new Scanner(System.in);
            num = in.nextInt();

            switch (num){
                case 1: System.out.println("Enter new dir"); hdfs.mkdirHDFS(in.next()); break;
                case 2: System.out.println("Enter new dir"); hdfs.mkdirLocalFS(in.next()); break;
                case 3: System.out.println("Enter loc file and hd file"); hdfs.upload(in.next(), in.next()); break;
                case 4: System.out.println("Enter loc file and hd file"); hdfs.download(in.next(), in.next()); break;
                case 5: System.out.println("Enter loc file , hd file , new file"); hdfs.merge(in.next(), in.next(), in.next()); break;
                case 6: System.out.println("Enter del file"); hdfs.delHDFS(in.next()); break;
                case 7: System.out.println("Enter del file"); hdfs.delLocalFS(in.next()); break;
                case 8: hdfs.lsHDFS(); break;
                case 9: hdfs.lsLocalFS(); break;
                case 10: System.out.println("Enter new path without '/' on end line or '..' "); hdfs.cdHDFS(in.next()); break;
                case 11: System.out.println("Enter new path without '/' on end line or '..' "); hdfs.cdLocalFS(in.next()); break;
            }
        }
    }
}
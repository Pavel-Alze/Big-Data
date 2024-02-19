package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class HDFS {

    Configuration conf = new Configuration();
    String localFS = "./";
    String tempLocalFS = "";
    String hdFS = "/home/redsu/mysources/";
    String tempHDFS = "";

    public void mkdirHDFS(String path){
        try{
            FileSystem fs = FileSystem.get(conf);
            fs.mkdirs(new Path(hdFS+tempHDFS+path));
            System.out.println(path + " Created ");
            fs.close();
        }catch (Exception e){System.out.println(e);}
    }

    public void mkdirLocalFS(String path){
        try{
            FileSystem fs = FileSystem.get(conf);
            fs.mkdirs(new Path(localFS+tempLocalFS+path));
            System.out.println(path + " Created ");
            fs.close();
        }catch (Exception e){System.out.println(e);}
    }

    public void upload(String locFile, String hdFile){
        try{
            FileSystem fs = FileSystem.get(conf);
            Path locPath = new Path(localFS+tempLocalFS+locFile);
            Path hdPath = new Path(hdFS+tempHDFS+hdFile);
            if(fs.exists(locPath) && fs.exists(hdPath) && fs.isFile(locPath) && fs.isFile(hdPath)){
                fs.copyFromLocalFile(false,true,locPath,hdPath);
                System.out.println("File uploaded");
            }else{System.out.println("Not file");}
            fs.close();
        }catch (Exception e){System.out.println(e);}
    }
    public void download(String locFile, String hdFile){
        try{
            FileSystem fs = FileSystem.get(conf);
            Path locPath = new Path(localFS+tempLocalFS+locFile);
            Path hdPath = new Path(hdFS+tempHDFS+hdFile);
            if(fs.exists(locPath) && fs.exists(hdPath) && fs.isFile(locPath) && fs.isFile(hdPath)){
                fs.copyToLocalFile(false,locPath,hdPath);
                System.out.println("File downloaded");
            }else{System.out.println("Not file");}
            fs.close();
        }catch (Exception e){System.out.println(e);}
    }

    public void merge(String locFile, String hdFile, String newFile){
        try{
            FileSystem fs = FileSystem.get(conf);
            Path locPath = new Path(localFS+tempLocalFS+locFile);
            Path hdPath = new Path(hdFS+tempHDFS+hdFile);
            Path newPath = new Path(hdFS+tempHDFS+newFile);
            if(fs.exists(locPath) && fs.exists(hdPath) && fs.isFile(locPath) && fs.isFile(hdPath)){
                FSDataInputStream inputStream = fs.open(locPath);
                byte[] bytes = new byte[(int) fs.getFileStatus(locPath).getLen()];
                inputStream.read(bytes);
                String localContent = new String(bytes, StandardCharsets.UTF_8);

                inputStream = fs.open(hdPath);
                bytes = new byte[(int) fs.getFileStatus(hdPath).getLen()];
                inputStream.read(bytes);
                String hdContent = new String(bytes, StandardCharsets.UTF_8);

                if (!fs.exists(newPath)) {
                    // Если файл не существует, создаем новый
                    OutputStream out = fs.create(newPath);
                    out.write(hdContent.getBytes(StandardCharsets.UTF_8));
                    out.write(("\n"+localContent).getBytes(StandardCharsets.UTF_8));
                    out.close();
                    System.out.println("File created with initial content.");
                } else {
                    // Открываем файл для добавления данных в конец
                    FSDataInputStream in = fs.open(newPath);
                    byte[] buffer = new byte[(int) fs.getFileStatus(newPath).getLen()];
                    in.readFully(0, buffer);
                    in.close();

                    String content = new String(buffer, StandardCharsets.UTF_8) + "\n" + hdContent + "\n" + localContent;

                    // Перезаписываем файл с новым содержимым
                    OutputStream out = fs.create(newPath, true);
                    out.write(content.getBytes(StandardCharsets.UTF_8));
                    out.close();
                    System.out.println("File created with initial content.");
                }
            }else{System.out.println("Not file");}
            fs.close();
        }catch (Exception e){System.out.println(e);}
    }

    public void delHDFS(String file){
        try{
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(hdFS+tempHDFS+file);
            if (fs.exists(path)) {
                fs.delete(path, true);
            }else {System.out.println("No file or dir");}
            fs.close();
        }catch (Exception e){System.out.println(e);}
    }

    public void delLocalFS(String file){
        try{
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(localFS+tempLocalFS+file);
            if (fs.exists(path)) {
                fs.delete(path, true);
            }else {System.out.println("No file or dir");}
            fs.close();
        }catch (Exception e){System.out.println(e);}
    }

    public void lsHDFS(){
        try{
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(hdFS+tempHDFS);
            if (fs.exists(path)) {
                FileStatus[] fileStatuses = fs.listStatus(path);
                for (FileStatus status : fileStatuses) {
                    if (status.isDirectory()) {
                        System.out.println("Directory in HDFS: " + status.getPath().getName());
                    } else {
                        System.out.println("File in HDFS: " + status.getPath().getName());
                    }
                }
            }else {System.out.println("No dir");}
            fs.close();
        }catch (Exception e){System.out.println(e);}
    }

    public void lsLocalFS(){
        try{
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(localFS+tempLocalFS);
            if (fs.exists(path)) {
                FileStatus[] fileStatuses = fs.listStatus(path);
                for (FileStatus status : fileStatuses) {
                    if (status.isDirectory()) {
                        System.out.println("Directory in LocalFS: " + status.getPath().getName());
                    } else {
                        System.out.println("File in LocalFS: " + status.getPath().getName());
                    }
                }
            }else {System.out.println("No dir");}
            fs.close();
        }catch (Exception e){System.out.println(e);}
    }

    public void cdHDFS(String newPath){
        try{
            FileSystem fs = FileSystem.get(conf);
            if(newPath.equals("..")){
                tempHDFS = "";
            }else {
                Path path = new Path(hdFS+tempHDFS+newPath);
                if(fs.exists(path)){
                    if(fs.isDirectory(path)){
                        tempHDFS+=newPath;
                        tempHDFS+="/";
                    }else {System.out.println("Not dir");}
                }else{System.out.println("No dir");}
            }
            fs.close();
            lsHDFS();
        }catch (Exception e){System.out.println(e);}
    }

    public void cdLocalFS(String newPath){
        try{
            FileSystem fs = FileSystem.get(conf);
            if(newPath.equals("..")){
                tempLocalFS = "";
            }else {
                Path path = new Path(localFS+tempLocalFS+newPath);
                if(fs.exists(path)){
                    if(fs.isDirectory(path)){
                        tempLocalFS+=newPath;
                        tempLocalFS+="/";
                    }else {System.out.println("Not dir");}
                }else{System.out.println("No dir");}
            }
            fs.close();
            lsLocalFS();
        }catch (Exception e){
            System.out.println(e);
        }
    }
    /*public void hd(){

        try {


            FileSystem fs = FileSystem.get(conf);


            fs.mkdirs(new Path("/home/redsu/mysources/WordCount/new_dir"));


            fs.copyFromLocalFile(false,true,new Path("src/main/resources/file.txt"),new Path("/home/redsu/mysources/WordCount/test.txt"));

            fs.copyToLocalFile(false, new Path("/home/redsu/mysources/WordCount/text.txt"),new Path("src/main/resources/file1.txt"), true);

            if (fs.exists(new Path("/home/redsu/mysources/WordCount/test.txt"))) {
                fs.delete(new Path("/home/redsu/mysources/WordCount/test.txt"), true);
            }

            FileStatus[] fileStatuses = fs.listStatus(new Path("/home/redsu/mysources/WordCount"));
            for (FileStatus status : fileStatuses) {
                if (status.isDirectory()) {
                    System.out.println("Directory in HDFS: " + status.getPath().getName());
                } else {
                    System.out.println("File in HDFS: " + status.getPath().getName());
                }
            }

            Path filePath = new Path("src/main/resources/file.txt");
            Path path = new Path("/home/redsu/mysources/WordCount/text.txt");
            FSDataInputStream inputStream = fs.open(path);
            byte[] bytes = new byte[(int) fs.getFileStatus(path).getLen()];
            inputStream.read(bytes);
            String content = new String(bytes, StandardCharsets.UTF_8);
            System.out.println(content);

            if (!fs.exists(filePath)) {
                // Если файл не существует, создаем новый
                OutputStream out = fs.create(filePath);
                out.write(content.getBytes(StandardCharsets.UTF_8));
                out.close();
                System.out.println("File created with initial content.");
            } else {
                // Открываем файл для добавления данных в конец
                FSDataInputStream in = fs.open(filePath);
                byte[] buffer = new byte[(int) fs.getFileStatus(filePath).getLen()];
                in.readFully(0, buffer);
                in.close();

                content = new String(buffer, StandardCharsets.UTF_8) + "\n" + content;

                // Перезаписываем файл с новым содержимым
                OutputStream out = fs.create(filePath, true);
                out.write(content.getBytes(StandardCharsets.UTF_8));
                out.close();
            }

            System.out.println("Data appended to the file successfully.");


            fileStatuses = fs.listStatus(new Path("."));
            for (FileStatus status : fileStatuses) {
                if (status.isDirectory()) {
                    System.out.println("Directory in HDFS: " + status.getPath().getName());
                } else {
                    System.out.println("File in HDFS: " + status.getPath().getName());
                }
            }

            fs.close();
        }catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }*/
}
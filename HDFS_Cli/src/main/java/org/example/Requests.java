package org.example;

import org.apache.http.HttpEntity;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;

import java.io.File;
import java.io.IOException;

public class Requests {

    public void get() throws IOException {

        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet("http://localhost:50070/webhdfs/v1/user/redsu/wordcount/input?user.name=redsu&op=LISTSTATUS");
        CloseableHttpResponse response = httpclient.execute(httpGet);
        try {
            System.out.println(response.getStatusLine());

            ResponseHandler<String> handler = new BasicResponseHandler();
            String body = httpclient.execute(httpGet, handler);
            System.out.println(body);
        }catch (Exception e){
            System.out.println(e);
        }finally {
            response.close();
        }

    }

    public void mkdir() throws IOException {
        CloseableHttpClient client = HttpClients.createDefault();
        HttpPut httpPut = new HttpPut("http://localhost:50070/webhdfs/v1/user/redsu/dir2?user.name=redsu&op=MKDIRS");

        CloseableHttpResponse response = client.execute(httpPut);
        try {
            System.out.println(response.getStatusLine());

            ResponseHandler<String> handler = new BasicResponseHandler();
            String body = client.execute(httpPut, handler);
            System.out.println(body);
        }catch (Exception e){
            System.out.println(e);
        }finally {
            response.close();
        }
    }

    public void create() throws IOException {
        CloseableHttpClient client = HttpClients.createDefault();
        HttpPut httpPut = new HttpPut("http://localhost:50070/webhdfs/v1/user/redsu/file?user.name=redsu&op=CREATE&overwrite=true&noredirect=true&replication=1");

        CloseableHttpResponse response = client.execute(httpPut);
        try {
            System.out.println(response.getStatusLine());

            ResponseHandler<String> handler = new BasicResponseHandler();
            String body = client.execute(httpPut, handler);
            System.out.println(body);
        }catch (Exception e){
            System.out.println(e);
        }finally {
            response.close();
        }
    }

    public void put() throws IOException {
        CloseableHttpClient client = HttpClients.createDefault();
        HttpPut httpPut = new HttpPut("http://debian:50075/webhdfs/v1/user/redsu/file?op=CREATE&user.name=redsu&namenoderpcaddress=localhost:9000&createflag=&createparent=true&overwrite=true&replication=1");

        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.addBinaryBody("file", new File("src/main/resources/file"),
                ContentType.APPLICATION_OCTET_STREAM, "file.ext");

        HttpEntity multipart = builder.build();
        httpPut.setEntity(multipart);

        CloseableHttpResponse response = client.execute(httpPut);
        try {
            System.out.println(response.getStatusLine());

            ResponseHandler<String> handler = new BasicResponseHandler();
            String body = client.execute(httpPut, handler);
            System.out.println(body);
        }catch (Exception e){
            System.out.println(e);
            System.out.println(response);
        }finally {
            response.close();
        }
    }
}

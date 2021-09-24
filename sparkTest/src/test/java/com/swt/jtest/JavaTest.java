package com.swt.jtest;

import com.kye.utils.KyeUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.jcodings.util.Hash;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class JavaTest {
    public static void main(String[] args) {
        long empty = Long.valueOf("");
        long nullNum = Long.valueOf(null);
        System.out.println("-----------------------------------------");
        //testEs();
        //testInteger();
    }

    public static void testEs(){
        //10.121.18.2:9200,10.121.18.4:9200,10.121.18.5:9200
        HttpHost httpHost1 = new HttpHost("10.121.18.2", 9200);
        HttpHost httpHost2 = new HttpHost("10.121.18.4", 9200);
        HttpHost httpHost3 = new HttpHost("10.121.18.5", 9200);

        BasicCredentialsProvider credentProvider = new BasicCredentialsProvider();
        credentProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "elastic@uat@123456"));
        RestClientBuilder builder = RestClient.builder(httpHost1, httpHost2, httpHost3)
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentProvider);
                    }
                });

        RestHighLevelClient highLevelClient = new RestHighLevelClient(builder);

        IndexRequest indexRequest = new IndexRequest("swt_test2", "type1");
        ArrayList<HashMap> sourceList = new ArrayList<>();
        HashMap<String, Object> source = new HashMap<>();
        BulkRequest bulkRequest = new BulkRequest();

        long startTime = System.currentTimeMillis();
        try {

            for (int i = 0; i < 10000; i++){

                //Thread.sleep(1);

                source.put("name", "Name " + System.currentTimeMillis() % 1000);
                source.put("age", System.currentTimeMillis() % 100);
                source.put("birthday", System.currentTimeMillis());

                //indexRequest.source(source)
                bulkRequest.add(indexRequest.source(source));
            }

            System.out.println("------------------- start to index ------------------------");
            //highLevelClient.index(indexRequest, RequestOptions.DEFAULT);
            highLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

            System.out.println("take time: " + (System.currentTimeMillis() - startTime));
        } catch (IOException e) {
            e.printStackTrace();
        } /*catch (InterruptedException e) {
            e.printStackTrace();
        }*/

        try {
            highLevelClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void splitToArrayList() {
        String str = "";

        String[] split = str.split(",", -1);

        List<String> stringList = Arrays.asList(split);
        if (stringList.contains("789")) {
            KyeUtils.println("contains ok...");
        }
    }

    public static void testInteger(){
        Integer i = (Integer)null;
        if (Integer.valueOf(0).equals(i))
            KyeUtils.println(i);
        else KyeUtils.println("not equals");
    }

}

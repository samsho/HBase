package com.hbase.moniter;

import com.alibaba.fastjson.JSON;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: MoniterCilent
 * Description:
 * Date: 2016/3/23 15:50
 *
 * @author sm12652
 * @version V1.0
 */
public class MoniterCilent {

    public static void main(String[] args) {
        String url = "http://hmaster:60010/jmx";
        String resStr = MoniterCilent.get(url);
        System.out.println("Response content: " + resStr);


    }


    /**
     * post方式提交表单（模拟用户登录请求）
     */
    public static void postForm() {

        String url = "http://localhost:8080/Java_WS_Server/rest/surpolicy/sendXml";

        // 创建默认的httpClient实例.
        HttpClient client = new DefaultHttpClient();
        // 创建httppost
        HttpPost httppost = new HttpPost(url);
        // 创建参数队列
        List<NameValuePair> formparams = new ArrayList<NameValuePair>();
        formparams.add(new BasicNameValuePair("username", "admin"));
        formparams.add(new BasicNameValuePair("password", "123456"));
        UrlEncodedFormEntity uefEntity;
        try {
            uefEntity = new UrlEncodedFormEntity(formparams, "UTF-8"); //编码
            httppost.setEntity(uefEntity);
            System.out.println("executing request " + httppost.getURI());
            HttpResponse response = client.execute(httppost);
            Header[] headers = response.getAllHeaders();
            for(int i=0; i<headers.length; i++){
                System.out.println(headers[i].getName());
            }

            try {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    System.out.println("--------------------------------------");
                    // 打印响应内容长度
                    System.out.println("Response content length: " + entity.getContentLength());
                    System.out.println("--------------------------------------");
                    System.out.println("Response content: " + EntityUtils.toString(entity, "UTF-8"));  //编码
                    System.out.println("--------------------------------------");
                }
            } finally {

            }
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e1) {
            e1.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

        }
    }


    /**
     * 发送 get请求
     */
    public static String get(String url) {
        CloseableHttpClient client = null;
        String resStr = null;
        try {
            client = HttpClientBuilder.create().build();
            // 创建httpget.
            HttpGet httpget = new HttpGet(url);
            System.out.println("executing request " + httpget.getURI());
            // 执行get请求.
            HttpResponse response = client.execute(httpget);
            // 获取响应实体
            HttpEntity entity = response.getEntity();
            System.out.println("--------------------------------------");
            // 打印响应状态
            System.out.println(response.getStatusLine());
            if (entity != null) {
                // 打印响应内容长度
                System.out.println("Response content length: " + entity.getContentLength());
                // 打印响应内容
                resStr = EntityUtils.toString(entity, "UTF-8");
            }
            client.close();

        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(client!=null) {
                try {
                    client.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
        return resStr;
    }
}

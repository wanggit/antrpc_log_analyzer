package io.github.wanggit.antrpc.log.analyzer.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticsearchConfiguration {

    @Value("${elasticsearch.uris}")
    private String uris;

    @Bean(name = "highLevelClient")
    public RestHighLevelClient highLevelClient() {
        return new RestHighLevelClient(RestClient.builder(makeHttpHost(uris)));
    }

    private HttpHost[] makeHttpHost(String uris) {
        if (null == uris) {
            throw new IllegalArgumentException();
        }
        String[] tmps = uris.split(",");
        HttpHost[] posts = new HttpHost[tmps.length];
        for (int i = 0; i < tmps.length; i++) {
            String[] address = tmps[i].trim().split(":");
            String ip = address[0];
            int port = Integer.parseInt(address[1]);
            posts[i] = new HttpHost(ip, port, "http");
        }
        return posts;
    }
}

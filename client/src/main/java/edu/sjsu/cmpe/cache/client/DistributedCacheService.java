package edu.sjsu.cmpe.cache.client;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;

import com.mashape.unirest.http.HttpResponse;


import com.mashape.unirest.http.JsonNode;


import java.util.concurrent.Future;

public class DistributedCacheService implements CacheServiceInterface {


    private final String Cache_Serurl;
    private CRDTCallbackInterface callback;

          public DistributedCacheService(String serverUrl) {
        this.Cache_Serurl = serverUrl;
    }

            public DistributedCacheService(String serverUrl, CRDTCallbackInterface cb) {
                      this.Cache_Serurl = serverUrl;
                       this.callback = cb;
    }

    @Override
    public String get(long k) {

        Future<HttpResponse<JsonNode>> future = Unirest.get(this.Cache_Serurl + "/cache/{key}")
          .header("accept", "application/json")
                .routeParam("key", Long.toString(k))
                .asJsonAsync(new Callback<JsonNode>() {

                    public void failed(UnirestException e) {
                        callback.getFailed(e);
                    }

                    public void completed(HttpResponse<JsonNode> response) {


                        callback.getCompleted(response, Cache_Serurl);
                    }

                    public void cancelled() {
                        System.out.println("request cancelled");
                    }

                });

        return null;
    }

    @Override
    public void put(long k, String value) {


        Future<HttpResponse<JsonNode>> future = Unirest.put(this.Cache_Serurl + "/cache/{key}/{value}")
                .header("accept", "application/json")
                .routeParam("key", Long.toString(k))
                .routeParam("value", value)
                .asJsonAsync(new Callback<JsonNode>() {

public void failed(UnirestException e) {

                        callback.putFailed(e);
                    }

                    public void completed(HttpResponse<JsonNode> response) {

                        callback.putCompleted(response, Cache_Serurl);
                    }

                    public void cancelled() {
                        System.out.println("Request Cancelled");
                    }

                });
    }

    @Override
    public void delete(long k) {


        HttpResponse<JsonNode> response = null;

    try {
            response = Unirest
                    .delete(this.Cache_Serurl + "/cache/{key}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(k))
                    .asJson();


        } catch (UnirestException e) {


            System.err.println(e);
        }

        System.out.println("response is " + response);

        if (response == null || response.getCode() != 204) {

   System.out.println("Failure");


        } else {
  System.out.println("Deleted " + k + " from " + this.Cache_Serurl);
        }

    }
}

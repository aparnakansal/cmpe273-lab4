package edu.sjsu.cmpe.cache.client;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;


public class CRDTClient implements CRDTCallbackInterface {
    private ConcurrentHashMap<String, ArrayList<String>> result;
    private ConcurrentHashMap<String, CacheServiceInterface> server;
    private ArrayList<String> server_success;
    private static CountDownLatch countDownLatch;



    public CRDTClient() {
        server = new ConcurrentHashMap<String, CacheServiceInterface>(3);

        CacheServiceInterface c0 = new DistributedCacheService("http://localhost:3000", this);

        CacheServiceInterface c1 = new DistributedCacheService("http://localhost:3001", this);

        CacheServiceInterface c2 = new DistributedCacheService("http://localhost:3002", this);

        server.put("http://localhost:3000", c0);

        server.put("http://localhost:3001", c1);

        server.put("http://localhost:3002", c2);
    }




    @Override
    public void putFailed(Exception e) {

        System.out.println("failed request");

        countDownLatch.countDown();
    }



    @Override
    public void putCompleted(HttpResponse<JsonNode> response, String serverUrl) {

        int code = response.getCode();

    System.out.println("response code " + code + "  server " + serverUrl);

          server_success.add(serverUrl);

   countDownLatch.countDown();
    }

    @Override
    public void getFailed(Exception e) {


  System.out.println("Failed Request");

        countDownLatch.countDown();
    }

    @Override
    public void getCompleted(HttpResponse<JsonNode> response, String serverUrl) {

        String value = null;

        if (response != null && response.getCode() == 200) {

            value = response.getBody().getObject().getString("value");

                System.out.println("value from server " + serverUrl + "is " + value);

      ArrayList serverWithValue = result.get(value);

            if (serverWithValue == null) {
                serverWithValue = new ArrayList(3);
            }


            serverWithValue.add(serverUrl);

           
    result.put(value, serverWithValue);
        }

      countDownLatch.countDown();
    }



    public boolean put(long key, String value) throws InterruptedException {


        server_success = new ArrayList(server.size());

        countDownLatch = new CountDownLatch(server.size());

        for (CacheServiceInterface cache : server.values()) {

            cache.put(key, value);
        }

        countDownLatch.await();

        boolean isSuccess = Math.round((float)server_success.size() / server.size()) == 1;

        if (! isSuccess) {
            
            delete(key, value);
        }


  return isSuccess;
    }

    public void delete(long key, String value) {

        for (final String serverUrl : server_success) {

            CacheServiceInterface servers = server.get(serverUrl);

    servers.delete(key);
        }
    }


    public String get(long key) throws InterruptedException {

        result = new ConcurrentHashMap<String, ArrayList<String>>();

        countDownLatch = new CountDownLatch(server.size());


        for (final CacheServiceInterface servers : server.values()) {
            servers.get(key);
        }


        countDownLatch.await();

       
               String rightValue = result.keys().nextElement();

        
        if (result.keySet().size() > 1 || result.get(rightValue).size() != server.size()) {
            
            ArrayList<String> vals = maxKeyForTable(result);

            if (vals.size() == 1) {
             
                rightValue = vals.get(0);
        ArrayList<String> repairserver = new ArrayList(server.keySet());
    repairserver.removeAll(result.get(rightValue));


     for (String serverUrl : repairserver) {
                    
          System.out.println("repairing: " + serverUrl + " value: " + rightValue);

                    CacheServiceInterface servers = server.get(serverUrl);

      servers.put(key, rightValue);

                }

            } else {
                
            }
        }

        return rightValue;

    }





    public ArrayList<String> maxKeyForTable(ConcurrentHashMap<String, ArrayList<String>> table) {


        ArrayList<String> key= new ArrayList<String>();

        int val = -1;

                 for(Map.Entry<String, ArrayList<String>> entry : table.entrySet()) {
                       if(entry.getValue().size() > val) {

                          key.clear();

                  key.add(entry.getKey());

                        val = entry.getValue().size();
            }
                      else if(entry.getValue().size() == val)
            {
                        key.add(entry.getKey());
            }
        }
                return key;

    }





}
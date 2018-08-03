package com.esspnews.dao;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;;


public class TestClient 
{
    public static String CLUSTER_NAME = "es-6.3-test";
    public static String HOST_IP = "0.0.0.0";
    public static int TCP_PORT = 9300;
	public static void main(String[] args) throws UnknownHostException
	{
		// Declare settings obj and assigning cluster name
        Settings settings = Settings.builder().put("cluster.name", CLUSTER_NAME).build();
        
        // TransportClient obj to connect with elasticsearch 
        TransportClient client = new PreBuiltTransportClient(settings)
        		                    .addTransportAddress(new TransportAddress(
        				             InetAddress.getByName(HOST_IP), TCP_PORT));
        
        // "TransportClient.prepareGet" method to get a document
        GetResponse getResponse = client.prepareGet("test", "type", "1").get();
        
        System.out.println(getResponse.getSourceAsString());	
	}//# void
}//#class
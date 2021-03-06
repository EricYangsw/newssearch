package com.esspnews.dao;

import com.esspnews.utils.EsUtils;
import org.elasticsearch.client.transport.TransportClient;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;


public class Dao {
	
	private Connection conn;
	
	// Method 1: void getConnection() to connect to mysql
	public void getConnection()
	{
		// connect to mysql
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            String user="root";
            String passwd="SoilWater66971025";
            String url="jdbc:mysql://localhost:3306/news?useSSL=false&serverTimezone=UTC";
            conn= DriverManager.getConnection(url,user,passwd);

            if (conn!=null){
                System.out.println("mysql連線成功!");
            }else{
                System.out.println("mysql連線失敗!");
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }// #void
	
	
	
	
	// Method 2: 
	public void mysqlToEs(){

        String sql="SELECT * FROM news";

        TransportClient client= EsUtils.getSingleClient();

        try {
            PreparedStatement pstm=conn.prepareStatement(sql);

            ResultSet resultSet=pstm.executeQuery();

            Map<String,Object> map=new HashMap<String, Object>();
            while (resultSet.next()){

                int nid=resultSet.getInt(1);

                map.put("id",nid);
                map.put("title",resultSet.getString(2));
                map.put("key_word",resultSet.getString(3));
                map.put("content",resultSet.getString(4));
                map.put("url",resultSet.getString(5));
                map.put("reply",resultSet.getInt(6));
                map.put("source",resultSet.getString(7));

                String postdatetime=resultSet.getTimestamp(8).toString();

                map.put("postdate",postdatetime.substring(0,postdatetime.length()-2));


                System.out.println(map);
                client.prepareIndex("search_news","news",String.valueOf(nid))
                        .setSource(map).execute().actionGet();

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
	
	
	


}// #class

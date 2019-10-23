package cn.edu.ruc;

import cn.edu.ruc.adapter.BaseAdapter;
import cn.edu.ruc.start.TSBM;

import com.alibaba.fastjson.JSON;
import okhttp3.*;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class DruidAdapter implements BaseAdapter{	
	private String writeURL = ":8200/v1/post/druidTest";
	private String queryURL = ":8082/druid/v2?pretty";
	
	MediaType MEDIA_TYPE_TEXT=MediaType.parse("application/json");
	private static final String LINE_SEPARATOR = System.getProperty("line.separator");
		
	private static final OkHttpClient OK_HTTP_CLIENT = new OkHttpClient().newBuilder()
	            .readTimeout(500000, TimeUnit.MILLISECONDS)
	            .connectTimeout(500000, TimeUnit.MILLISECONDS)
	            .writeTimeout(500000, TimeUnit.MILLISECONDS)
	            .build();

	 public static OkHttpClient getOkHttpClient() {
	        return OK_HTTP_CLIENT;
	    }
	 private long exeOkHttpRequest(Request request) {
	        long costTime = 0L;
	        Response response;
	        OkHttpClient client = getOkHttpClient();
	        try {
	            long startTime1 = System.nanoTime();
	            response = client.newCall(request).execute();
	            int code = response.code();
//                System.out.println(response.body().string());
	            response.close();
	            long endTime1 = System.nanoTime();
	            costTime = endTime1 - startTime1;
	        } catch (Exception e) {
	            e.printStackTrace();
	            return -1;
	        }
	        return costTime / 1000 / 1000;
	    }
	  public void initConnect(String ip, String port, String user, String password) {
			// TODO Auto-generated method stub
			writeURL="http://"+ip+writeURL;
			queryURL="http://"+ip+queryURL;
		}
	  public long insertData(String data) {
	        String[] rows = data.split(TSBM.LINE_SEPARATOR);
	        StringBuilder sc = new StringBuilder();
          List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();

	        for (String row : rows) {
	            String[] sensors = row.split(TSBM.SEPARATOR);
	            if (sensors.length < 3) {//过滤空行
	                continue;
	            }
	           
	            String timestamp = sensors[0];
	            String farmId = sensors[1];
	            String deviceId = sensors[2];
	            int length = sensors.length;
	            for (int index = 3; index < length; index++) {
	            	Map<String,Object> pointMap=new HashMap<>();
	            	String value = sensors[index];
	                String sensorName = "s" + (index - 2);
//	                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
//                    String tm = format.format(new Date(timestamp));
	                pointMap.put("time", timestamp);
	                pointMap.put("f", farmId);
	                pointMap.put("d", deviceId);
	                pointMap.put("s", sensorName);
		            pointMap.put("value", value);  
		            list.add(pointMap);		           
	            }
	           
	        }
	  
	        String json=JSON.toJSONString(list);
	        Request request=null;
	        try{
	        	request = new Request.Builder()
		            .url(writeURL)
		            .post(RequestBody.create(MEDIA_TYPE_TEXT, json.toString().getBytes("UTF-8")))
		            .build();
	        }catch(Exception e) {
	        	e.printStackTrace();
	        }

	        return exeOkHttpRequest(request);	
	  }
	@Override
	public long query1(long start_time, long end_time) {
		// TODO Auto-generated method stub
		Map<String,Object> map=new HashMap<String,Object>();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        String start = format.format(start_time);
        String end=format.format(end_time);
        String array_time[]=new String[1];
        array_time[0]=start+"/"+end;
        map.put("queryType", "select");
        map.put("dataSource", "druidTest");
        Object array_dimensions[]=new Object[1];
        map.put("dimensions",new ArrayList<Object>());
        Object a[]=new Object[1];
        map.put("metrics",new ArrayList<Object>());
        Object fields[]=new Object[3];
        Map<String,Object> json1=new HashMap<String,Object>();
        Map<String,Object> json2=new HashMap<String,Object>();
        Map<String,Object> json3=new HashMap<String,Object>();
        json1.put("type", "selector");
        json2.put("type", "selector");
        json3.put("type", "selector");
        json1.put("dimension","f");
        json2.put("dimension","d");
        json3.put("dimension", "s");
        json1.put("value", "f1");
        json2.put("value", "d2");
        json3.put("value", "s1");
        fields[0]=json1;
        fields[1]=json2;
        fields[2]=json3;
        Map<String,Object> filter=new HashMap<String,Object>();
        filter.put("type","and");
        filter.put("fields",fields);
        map.put("filter", filter);
        map.put("granularity", "all");
        map.put("intervals", array_time);
        Map<String,Object> pagingSpec=new HashMap<String,Object>();
        Map<String,Object> null_page=new HashMap<String,Object>();
        pagingSpec.put("pagingIdentifiers", null_page);
        pagingSpec.put("threshold", 5);
        map.put("pagingSpec", pagingSpec);
        String json=JSON.toJSONString(map);
        Request request=null;
        try {
            request = new Request.Builder()
                    .url(queryURL)
                    .post(RequestBody.create(MEDIA_TYPE_TEXT, json.toString().getBytes("UTF-8")))
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return exeOkHttpRequest(request);
	}
	@Override
	public long query2(long start_time, long end_time, double VALUE) {
		// TODO Auto-generated method stub
		Map<String,Object> map=new HashMap<String,Object>();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        String start = format.format(start_time);
        String end=format.format(end_time);
        String array_time[]=new String[1];
        array_time[0]=start+"/"+end;
        
        map.put("queryType", "select");
        map.put("dataSource", "druidTest");
        Object array_dimensions[]=new Object[1];
        map.put("dimensions",new ArrayList<Object>());
        Object a[]=new Object[1];
        map.put("metrics",new ArrayList<Object>());
        Object fields[]=new Object[3];
        Map<String,Object> json1=new HashMap<String,Object>();
        Map<String,Object> json2=new HashMap<String,Object>();
        Map<String,Object> json3=new HashMap<String,Object>();
        json1.put("type", "selector");
        json2.put("type", "selector");
        json3.put("type", "bound");
        json1.put("dimension","f");
        json2.put("dimension","d");
        json3.put("dimension", "value");
        json1.put("value", "f1");
        json2.put("value", "d2");
        json3.put("lower", VALUE);
      //json3.put("lowerStrict":true);  
        fields[0]=json1;
        fields[1]=json2;
        fields[2]=json3;
        Map<String,Object> filter=new HashMap<String,Object>();
        filter.put("type","and");
        filter.put("fields",fields);
        map.put("filter", filter);
        map.put("granularity", "all");
        map.put("intervals", array_time);
        Map<String,Object> pagingSpec=new HashMap<String,Object>();
        Map<String,Object> null_page=new HashMap<String,Object>();
        pagingSpec.put("pagingIdentifiers", null_page);
        pagingSpec.put("threshold", 5);
        map.put("pagingSpec", pagingSpec);
        
        String json=JSON.toJSONString(map);
        Request request=null;
        try {
            request = new Request.Builder()
                    .url(queryURL)
                    .post(RequestBody.create(MEDIA_TYPE_TEXT, json.toString().getBytes("UTF-8")))
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return exeOkHttpRequest(request);
	}
	@Override
	public long query3(long start_time, long end_time) {
		// TODO Auto-generated method stub
		Map<String,Object> map=new HashMap<String,Object>();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        String start = format.format(start_time);
        String end=format.format(end_time);
        String array_time[]=new String[1];
        array_time[0]=start+"/"+end;
        
        map.put("queryType", "groupBy");
        map.put("dataSource", "druidTest");
        map.put("granularity", "hour");
       
        String array_dimensions[]=new String[3];
        array_dimensions[0]="f";
        array_dimensions[1]="d";
        array_dimensions[2]="s";
        map.put("dimensions",array_dimensions);
        Map<String,Object> json1=new HashMap<String,Object>();
        json1.put("type", "selector");
        json1.put("dimension","f");
        json1.put("value", "f1");
        map.put("filter", json1);
        
        //aggregation
        Object aggregations[]=new Object[3];
        Object aggregations_avg[]=new Object[2];

        Map<String,Object> count=new HashMap<String,Object>();
        Map<String,Object> doubleSum=new HashMap<String,Object>();
        Map<String,Object> doubleX=new HashMap<String,Object>();
        count.put("type" , "count");
        count.put("name" , "count");
        count.put("fieldName","value");
        doubleSum.put("type", "doubleSum");
        doubleSum.put("name", "sum");
        doubleSum.put("fieldName", "value");
        aggregations_avg[0]=count;
        aggregations_avg[1]=doubleSum;
        //aggregations[2]=doubleX;
        map.put("aggregations",aggregations_avg);
        Map<String,Object> postAggregations=new HashMap<String,Object>();
        Object fields2[]=new Object[2];
        Map<String,Object> json3=new HashMap<String,Object>();
        Map<String,Object> json4=new HashMap<String,Object>();
        json3.put("type", "fieldAccess");
        json4.put("type", "fieldAccess");
        json3.put("fieldName","sum");
        json4.put("fieldName","count");
        fields2[0]=json3;
        fields2[1]=json4;
        postAggregations.put("type","arithmetic");
        postAggregations.put("name","avg");
        postAggregations.put("fn", "/");
        postAggregations.put("fields", fields2);
        Object a[]=new Object[1];
        a[0]=postAggregations;
        map.put("postAggregations", a);
        map.put("intervals", array_time);
        
        String json=JSON.toJSONString(map);
       // System.out.println(json);
        Request request=null;
        try {
            request = new Request.Builder()
                    .url(queryURL)
                    .post(RequestBody.create(MEDIA_TYPE_TEXT, json.toString().getBytes("UTF-8")))
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return exeOkHttpRequest(request);
 
	//	return 0;
	}
	@Override
	public long query4(long start_time, long end_time) {
		// TODO Auto-generated method stub
		Map<String,Object> map=new HashMap<String,Object>();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        String start = format.format(start_time);
        String end=format.format(end_time);
        String array_time[]=new String[1];
        array_time[0]=start+"/"+end;
        map.put("queryType", "select");
        map.put("dataSource", "druidTest");
        Object array_dimensions[]=new Object[1];
        map.put("dimensions",new ArrayList<Object>());
        Object a[]=new Object[1];
        map.put("metrics",new ArrayList<Object>());
        Object fields[]=new Object[2];
        Map<String,Object> json1=new HashMap<String,Object>();
        Map<String,Object> json2=new HashMap<String,Object>();
        json1.put("type", "selector");
        json2.put("type", "in");
        json1.put("dimension","f");
        json2.put("dimension","s");
        json1.put("value", "f1");
        String set[]= {"s1","s2","s3","s4","s5"};
        json2.put("value", set);
        fields[0]=json1;
        fields[1]=json2;
        Map<String,Object> filter=new HashMap<String,Object>();
        filter.put("type","and");
        filter.put("fields",fields);
        map.put("filter", filter);
        map.put("granularity", "all");
        map.put("intervals", array_time);
        Map<String,Object> pagingSpec=new HashMap<String,Object>();
        Map<String,Object> null_page=new HashMap<String,Object>();
        pagingSpec.put("pagingIdentifiers", null_page);
        pagingSpec.put("threshold", 5);
        map.put("pagingSpec", pagingSpec);
        String json=JSON.toJSONString(map);
        System.out.println(json);
        Request request=null;
        try {
            request = new Request.Builder()
                    .url(queryURL)
                    .post(RequestBody.create(MEDIA_TYPE_TEXT, json.toString().getBytes("UTF-8")))
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return exeOkHttpRequest(request);
		
	}
	@Override
	public long query5(long start_time, long end_time) {
		// TODO Auto-generated method stub
		Map<String,Object> map=new HashMap<String,Object>();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        String start = format.format(start_time);
        String end=format.format(end_time);
        String array_time[]=new String[1];
        array_time[0]=start+"/"+end;
        map.put("queryType", "select");
        map.put("dataSource", "druidTest");
        Object array_dimensions[]=new Object[1];
        map.put("dimensions",new ArrayList<Object>());
        Object a[]=new Object[1];
        map.put("metrics",new ArrayList<Object>());
        Map<String,Object> json1=new HashMap<String,Object>();
        json1.put("type", "selector");
        json1.put("dimension","f");
        json1.put("value", "f1");
        map.put("filter", json1);
        map.put("granularity", "all");
        map.put("intervals", array_time);
        Map<String,Object> pagingSpec=new HashMap<String,Object>();
        Map<String,Object> null_page=new HashMap<String,Object>();
        pagingSpec.put("pagingIdentifiers", null_page);
        pagingSpec.put("threshold", 5);
        map.put("pagingSpec", pagingSpec);
        String json=JSON.toJSONString(map);
        System.out.println(json);
        Request request=null;
        try {
            request = new Request.Builder()
                    .url(queryURL)
                    .post(RequestBody.create(MEDIA_TYPE_TEXT, json.toString().getBytes("UTF-8")))
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return exeOkHttpRequest(request);
	}
	  
	  public static void main(String []args) {
		  DruidAdapter a=new DruidAdapter();
		  a.query1(3, 5);
	  }
	  
	  
}

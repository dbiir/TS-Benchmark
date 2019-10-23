package cn.edu.ruc;


import com.alibaba.fastjson.JSON;
import cn.edu.ruc.adapter.BaseAdapter;
import cn.edu.ruc.start.TSBM;
import okhttp3.*;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.*;
import java.util.concurrent.TimeUnit;


public class OpentsdbAdapter implements BaseAdapter{
	 private String writeURL = "/api/put";
	 private String queryURL = "/api/query";
	 private String METRIC = "wind.perform";
	 private String FARM_TAG="FARM_TAG";
	 private String DEVICE_TAG="DEVICE_TAG";
	 private String SENSOR_TAG="SENSOR_TAG";
	 MediaType MEDIA_TYPE_TEXT = MediaType.parse("text/plain");
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
//				System.out.println(response.body().string());
	            response.close();
	            long endTime1 = System.nanoTime();
	            costTime = endTime1 - startTime1;
	        } catch (Exception e) {
	            e.printStackTrace();
	            return -1;
	        }
	        return costTime / 1000 / 1000;
	    }
	    
		@Override
		public void initConnect(String ip, String port, String user, String password) {
			// TODO Auto-generated method stub
			writeURL="http://"+ip+":"+port+""+writeURL;
			queryURL="http://"+ip+":"+port+""+queryURL;
		}
	    
	    //执行insert
	    public long insertData(String data) {
	        String[] rows = data.split(TSBM.LINE_SEPARATOR);
	        StringBuilder sc = new StringBuilder();
            List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();

	        for (String row : rows) {
	            String[] sensors = row.split(TSBM.SEPARATOR);
	            if (sensors.length < 3) {//过滤空行
	                continue;
	            }
//	            Map<String,Object> pointMap=new HashMap<>();
	            String timestamp = sensors[0];
	            String farmId = sensors[1];
	            String deviceId = sensors[2];
	            int length = sensors.length;
	            for (int index = 3; index < length; index++) {
					Map<String,Object> pointMap=new HashMap<>();
	                String value = sensors[index];
	                String sensorName = "s" + (index - 2);
	                pointMap.put("metric", METRIC);
		            pointMap.put("timestamp", timestamp);
		            pointMap.put("value", value);  
		            Map<String,Object> Tags=new HashMap<>();
		            Tags.put(FARM_TAG,farmId);
		            Tags.put(DEVICE_TAG, deviceId);
		            Tags.put(SENSOR_TAG, sensorName);
	                pointMap.put("tags", Tags);
	                list.add(pointMap);
	            }
	           
	        }
	        String json=JSON.toJSONString(list);
	        Request request = new Request.Builder()
		            .url(writeURL)
		            .post(RequestBody.create(MEDIA_TYPE_TEXT, json.toString()))
		            .build();
	        return exeOkHttpRequest(request);
	       }
	    
	 //执行query
	 public long execQuery(String query) {
		    Request request = new Request.Builder()
		            .url(queryURL)
		            .post(RequestBody.create(MEDIA_TYPE_TEXT, query))
		            .build();
			return exeOkHttpRequest(request);
	 }


	
	@Override
	public long query1(long start, long end) {
		// TODO Auto-generated method stub
		Map<String,Object> map = new  HashMap<String,Object>();
		map.put("start",start);
		map.put("end", end);
		Map<String,Object> subQuery = new HashMap<String ,Object>();
		subQuery.put("aggregator","avg");
		subQuery.put("metric", METRIC);
		Map<String,Object> subTag = new HashMap<String ,Object>();
		subTag.put(FARM_TAG, "f1");
		subTag.put(DEVICE_TAG,"d2");
		subTag.put(SENSOR_TAG,"s1");
		subQuery.put("tags", subTag);
		List<Map<String,Object>> list2 = new ArrayList<>();
		list2.add(subQuery);
		map.put("queries", list2);
		String json=JSON.toJSONString(map);
	//	System.out.println(json);
		return execQuery(json);
	}
	@Override
	public long query2(long arg0, long arg1, double arg2) {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	public long query3(long start, long end) {
		// TODO Auto-generated method stub
		Map<String,Object> map = new  HashMap<String,Object>();
		map.put("start",start);
		map.put("end", end);
		Map<String,Object> subQuery = new HashMap<String ,Object>();
		subQuery.put("aggregator","avg");
		subQuery.put("metric", METRIC);
		subQuery.put("downsample", "1h-avg");
		Map<String,Object> subTag = new HashMap<String ,Object>();
		subTag.put(FARM_TAG,"f1");
		subQuery.put("tags", subTag);
		List<Map<String,Object>> list2 = new ArrayList<>();
		list2.add(subQuery);
		map.put("queries", list2);
		String json=JSON.toJSONString(map);
	//	System.out.println(json);
		return execQuery(json);
	}
	public long query4(long start, long end) {
		// TODO Auto-generated method stub
		Map<String,Object> map = new  HashMap<String,Object>();
		map.put("start",start);
		map.put("end", end);
		Map<String,Object> subQuery = new HashMap<String ,Object>();
		subQuery.put("aggregator","avg");
		subQuery.put("metric", METRIC);
		Map<String,Object> subTag = new HashMap<String ,Object>();
		subTag.put(FARM_TAG,"f1");
		subTag.put(DEVICE_TAG, "*");
		subTag.put(SENSOR_TAG, "s1|s2|s3|s4|s5");
		subQuery.put("tags", subTag);
		List<Map<String,Object>> list2 = new ArrayList<>();
		list2.add(subQuery);
		map.put("queries", list2);
		String json=JSON.toJSONString(map);
		System.out.println(json);
		return execQuery(json);
	}
	@Override
	public long query5(long start, long end) {
		// TODO Auto-generated method stub
		Map<String,Object> map = new  HashMap<String,Object>();
		map.put("start",start);
		map.put("end", end);
		Map<String,Object> subQuery = new HashMap<String ,Object>();
		subQuery.put("aggregator","avg");
		subQuery.put("metric", METRIC);
		Map<String,Object> subTag = new HashMap<String ,Object>();
		subTag.put(FARM_TAG, "f1");
		subQuery.put("tags", subTag);
		List<Map<String,Object>> list2 = new ArrayList<>();
		list2.add(subQuery);
		map.put("queries", list2);
		String json=JSON.toJSONString(map);
		//System.out.println(json);
		return execQuery(json);
	}
	public static void main(String []args) {
//		OpentsdbAdapter a=new OpentsdbAdapter();
//		a.initConnect("10.77.110.226","4242","root","root");
//		String json="{\"start\":1514736000000,\"end\":1514739600000,\"queries\":[{\"metric\":\"wind.perform\",\"aggregator\":\"avg\",\"tags\":{\"SENSOR_TAG\":\"s1\",\"DEVICE_TAG\":\"d2\",\"FARM_TAG\":\"f1\"}}]}\n";
//		String json ="";
//		long l = a.query4(1514736000000L, 1514739600000L);
//		System.out.println(l);
	}

}

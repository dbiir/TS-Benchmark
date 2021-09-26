package cn.edu.ruc;

import cn.edu.ruc.adapter.BaseAdapter;
import cn.edu.ruc.start.TSBM;
import okhttp3.*;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.Point;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.QueryResult;

import javax.net.ssl.*;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.*;

// get set control + return
//自动删除无效引用  control+option+o
//格式化 Command + Option + L
public class AliInfluxAdapter implements BaseAdapter {// ctrl+i 快速实现接口

    public static SSLSocketFactory socketFactory() {
        SSLSocketFactory ssfFactory = null;

        try {
            SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(null, new TrustManager[]{new trustManager()}, new SecureRandom());

            ssfFactory = sc.getSocketFactory();
        } catch (Exception e) {
        }

        return ssfFactory;
    }

    public static class hostnameVerifier implements HostnameVerifier {
        @Override
        public boolean verify(String hostname, SSLSession session) {
            return true;
        }
    }

    public static class trustManager implements X509TrustManager, TrustManager {
        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }

        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {

        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }

    private String writeURL = "";
    private String queryURL = "";
    private String dbName = "ruc_test";
    MediaType MEDIA_TYPE_TEXT = MediaType.parse("text/plain");
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");
    private org.influxdb.InfluxDB INFLUXDB = null;
    //private Queue<org.influxdb.InfluxDB> queue = new LinkedList<org.influxdb.InfluxDB>();
    private static final OkHttpClient OK_HTTP_CLIENT = new OkHttpClient().newBuilder()
            .readTimeout(50000, TimeUnit.MILLISECONDS)
            .connectTimeout(50000, TimeUnit.MILLISECONDS)
            .writeTimeout(50000, TimeUnit.MILLISECONDS)
            .retryOnConnectionFailure(true)
            .connectionSpecs(Arrays.asList(ConnectionSpec.MODERN_TLS, ConnectionSpec.COMPATIBLE_TLS))
            .sslSocketFactory(gaussDBForInfluxAdapter.socketFactory(), new gaussDBForInfluxAdapter.trustManager())
            .hostnameVerifier(new gaussDBForInfluxAdapter.hostnameVerifier())
            .build();

    public long execQuery(String query) {
        System.out.println("Query: " + query);
        long costTime = 0L;
        try {
            long startTime1 = System.nanoTime();
            QueryResult results = INFLUXDB.query(new Query(query, dbName));
            long endTime1 = System.nanoTime();
            costTime = endTime1 - startTime1;
            if (results.hasError()) {
                return -1;
            } else {
                return costTime / 1000 / 1000;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }

    public long query1(long start, long end) {
        String sql = "select * from sensor where f='%s' and d='%s' and s='%s' and time>=%s and time<=%s";
        String eSql = String.format(sql, "f1", "d2", "s1", TimeUnit.MILLISECONDS.toNanos(start),
                TimeUnit.MILLISECONDS.toNanos(end));
        return execQuery(eSql);
    }

    public long query2(long start, long end, double value) {
        String sqlFormat = "select * from sensor where f='%s' and s='%s' and value>=%s and time>=%s and time<%s";
        String eSql = String.format(sqlFormat, "f1", "d2", value,
                TimeUnit.MILLISECONDS.toNanos(start),
                TimeUnit.MILLISECONDS.toNanos(end));
        return execQuery(eSql);
    }

    public long query3(long start, long end) {
        String sqlFormat = "select mean(value) from sensor where f='%s' and s='%s' and time>=%s and time<=%s group by " +
                "f,d,s,time(1h)";
        String eSql = String.format(sqlFormat, "f1", "s1", TimeUnit.MILLISECONDS.toNanos(start),
                TimeUnit.MILLISECONDS.toNanos(end));
        System.out.println(eSql);
        return execQuery(eSql);
    }

    public long query4(long start, long end) {
        String sqlFormat = "select * from sensor where f='%s' and (s='%s' or s='%s' or s='%s' or s='%s' or s='%s') " +
                "and time>=%s and time<=%s";
        String eSql = String.format(sqlFormat, "f1", "s1", "s2", "s3", "s4", "s5", TimeUnit.MILLISECONDS.toNanos(start),
                TimeUnit.MILLISECONDS.toNanos(end));
        return execQuery(eSql);
    }

    public long query5(long start, long end) {
        String sqlFormat = "select * from sensor where f='%s' and time>=%s and time<=%s";
        String eSql = String.format(sqlFormat, "f1", TimeUnit.MILLISECONDS.toNanos(start),
                TimeUnit.MILLISECONDS.toNanos(end));
        return execQuery(eSql);
    }

    public void initConnect(String ip, String port, String user, String password) {
        String baseUrl = String.format("https://%s:%s@%s:%s", user, password, ip, port);
        this.writeURL = baseUrl + "/write?precision=ms&db=" + dbName;
        this.queryURL = baseUrl + "/query?db=" + dbName;

        try {
            OkHttpClient.Builder client = new OkHttpClient().newBuilder()
                    .readTimeout(50000, TimeUnit.MILLISECONDS)
                    .connectTimeout(50000, TimeUnit.MILLISECONDS)
                    .writeTimeout(50000, TimeUnit.MILLISECONDS)
                    .connectionSpecs(Arrays.asList(ConnectionSpec.MODERN_TLS, ConnectionSpec.COMPATIBLE_TLS))
                    .sslSocketFactory(gaussDBForInfluxAdapter.socketFactory(), new gaussDBForInfluxAdapter.trustManager())
                    .hostnameVerifier(new gaussDBForInfluxAdapter.hostnameVerifier());
            INFLUXDB = InfluxDBFactory.connect(baseUrl, user, password, client);
        } catch (Exception e) {
            System.out.print("Initialize InfluxDB failed because ");
            System.out.println(e.toString());
        }
        INFLUXDB.setDatabase(dbName);
        INFLUXDB.createDatabase(dbName);
        INFLUXDB.enableBatch(5000, 1000, TimeUnit.MILLISECONDS);
    }

    /*
    public org.influxdb.InfluxDB getConnection() {
        synchronized (this) {
            while (true) {
                if (queue.isEmpty()) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }
                org.influxdb.InfluxDB conn = queue.poll();
                if (conn == null) {
                    continue;
                }
                return conn;
            }
        }
    }
    */
    public long insertData(String data) {
        String[] rows = data.split(TSBM.LINE_SEPARATOR);
        StringBuilder sc = new StringBuilder();
        BatchPoints batchPoints = BatchPoints.database(dbName).build();
        int turn = 0;
        long costTime = 0L;
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
                String value = sensors[index];
                String sensorName = "s" + (index - 2);
                Point point = Point.measurement("sensor")
                        .time(Long.valueOf(timestamp), TimeUnit.MILLISECONDS)
                        .tag("f", farmId)
                        .tag("d", deviceId)
                        .tag("s", sensorName)
                        .addField("value", Float.valueOf(value))
                        .build();
                batchPoints.point(point);
            }
            turn++;
            if (turn == 10) {
                turn = 0;
                long startTime = System.nanoTime();
                INFLUXDB.write(batchPoints);
                batchPoints = BatchPoints.database(dbName).build();
                long endTime = System.nanoTime();
                costTime += (endTime - startTime) / 1000000;
            }
        }
        if (turn != 0) {
            long startTime = System.nanoTime();
            INFLUXDB.write(batchPoints);
            long endTime = System.nanoTime();
            costTime += (endTime - startTime) / 1000000;
        }
        return costTime;
    }
}

package cn.edu.ruc;

import cn.edu.ruc.adapter.BaseAdapter;
import cn.edu.ruc.start.TSBM;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import java.util.ArrayList;
import java.util.List;

/**
 * iotdb test
 */
public class IotdbAdapterNativeApi implements BaseAdapter {
    private Session session;
    private String rootSeries = "root.p";

    public void initConnect(String ip, String port, String user, String password) {
        session = new Session(ip, port, user, password);
        try {
            session.open(false);
        } catch (IoTDBConnectionException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void clearAll(List<List<String>> measurementsList,
                          List<List<Object>> valuesList,
                          List<Long> timestamps,
                          List<String> paths,
                          List<List<TSDataType>> typesList) {
        measurementsList.clear();
        valuesList.clear();
        timestamps.clear();
        paths.clear();
        typesList.clear();
    }

    public long insertData(String data) {
        String[] rows = data.split(TSBM.LINE_SEPARATOR);
        Long costTime = 0L;
        Long cost = 0L;
        System.out.println("start insert " + rows.length * 50 + " Points");
        Long count = 0L;
        String format = "%s.%s.%s";
        List<List<String>> measurementsList = new ArrayList<List<String>>();
        List<List<Object>> valuesList = new ArrayList<List<Object>>();
        List<List<TSDataType>> typesList = new ArrayList<List<TSDataType>>();
        List<Long> timestamps = new ArrayList<Long>();
        List<String> paths = new ArrayList<String>();
        int turn = 0;
        for (String row : rows) {
            String[] sensors = row.split(TSBM.SEPARATOR);
            if (sensors.length < 3) {//过滤空行
                continue;
            }
            String timestamp = sensors[0];
            String farmId = sensors[1];
            String deviceId = sensors[2];
            String path = String.format(format, rootSeries, farmId, deviceId);

            int length = sensors.length;
            for (int index = 3; index < length; index++) {
                List<String> measurements = new ArrayList<>();
                List<TSDataType> types = new ArrayList<>();
                List<Object> values = new ArrayList<>();
                String value = sensors[index];
                String sensorName = "s" + (index - 2);
                measurements.add(sensorName);
                types.add(TSDataType.FLOAT);
                values.add(Float.valueOf(value));
                measurementsList.add(measurements);
                typesList.add(types);
                valuesList.add(values);
                timestamps.add(Long.valueOf(timestamp));
                paths.add(path);
            }
            turn++;
            if( turn == 5) {
                turn = 0;
                long startTime = System.nanoTime();
                try {
                    session.insertRecords(paths, timestamps, measurementsList, typesList, valuesList);
                } catch (IoTDBConnectionException e) {
                    e.printStackTrace();
                } catch (StatementExecutionException e) {
                    e.printStackTrace();
                }
                long endTime = System.nanoTime();
                cost = (endTime - startTime) / 1000 / 1000;
                System.out.println("Insert "+valuesList.size()+" Points, Use Time: " + cost + "ms");
                clearAll(measurementsList, valuesList, timestamps, paths, typesList);
                costTime += cost;
            }
        }
        return costTime;
    }

    public long query1(long start, long end) {
        String formatSql = "select %s from  %s.%s.%s  where time>=%s and time<=%s";
        String eSql = String.format(formatSql, "s1", rootSeries, "f1", "d1", start, end);
        System.out.println(eSql);
        return execQuery(eSql);
    }

    public long query2(long start, long end, double value) {
        //select s1 from root.perform.f1.* where s1>0;
        String formatSql = "select %s from  %s.%s.*  where time>=%s and time<=%s and %s>%s";
        String eSql = String.format(formatSql, "s1", rootSeries, "f1", start, end, "s1", value);
        System.out.println(eSql);
        return execQuery(eSql);
    }

    public long query3(long start, long end) {
        //select mean(*) from root.perform.f1.* group by (1h,[0,100000000]);
        String formatSql = "select mean(s1) from  %s.%s.* where time>=%s and time<=%s group by " +
                "(1h,[%s,%s])";
        String eSql = String.format(formatSql, rootSeries, "f1", start, end, start, end);
        System.out.println(eSql);
        return execQuery(eSql);
    }

    public long query4(long start, long end) {
        //select s1,s2 from root.perform.f1.* ;
        String formatSql = "select %s,%s,%s,%s,%s from %s.%s.* where time>=%s and time<=%s";
        String eSql = String.format(formatSql, "s1", "s2", "s3", "s4", "s5", rootSeries, "f1", start, end);
        System.out.println(eSql);
        return execQuery(eSql);
    }

    public long query5(long start, long end) {
        //select * from root.perform.f1.* ;
        String formatSql = "select * from %s.%s.* where time>=%s and time<=%s";
        String eSql = String.format(formatSql, rootSeries, "f1", start, end);
        System.out.println(eSql);
        return execQuery(eSql);
    }

    public long execQuery(String sql) {
        long costTime = 0;
        long startTime = System.nanoTime();
        try {
            SessionDataSet dataSet = session.executeQueryStatement(sql);
            long endTime = System.nanoTime();
            costTime = endTime - startTime;
            if (dataSet.getColumnNames().size() == 0) {
                System.out.println("Query Nothing");
            }
            dataSet.closeOperationHandle();
        } catch (StatementExecutionException e) {
            e.printStackTrace();
        } catch (IoTDBConnectionException e) {
            e.printStackTrace();
        }
        return costTime / 1000 / 1000;
    }
}

package cn.edu.ruc;

import cn.edu.ruc.adapter.BaseAdapter;
import cn.edu.ruc.start.TSBM;
import cn.edu.ruc.kx.c;
import java.io.IOException;
import cn.edu.ruc.kx.c.KException;
import java.sql.Timestamp;
import java.util.*;

public class KdbPlusAdapter implements BaseAdapter {
    private String host;
    private int port;
    private String username;
    private String password;
    private boolean useTLS;
    private String dbName = "ruc_test";

    public c getConnection() {
        try {
            return new c(host, port, username + ":" + password, useTLS);
        } catch (IOException | KException e) {
            e.printStackTrace();
            return null;
        }
    }

    public c getDefaultConnection() {
        try {
            return new c("127.0.0.1", 5001);
        } catch (IOException | KException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void closeConnection(c c) {
        if (c != null)
            try {
                c.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    @Override
    public void initConnect(String ip, String port, String user, String password) {
        this.host = ip;
        this.port = Integer.parseInt(port);
        this.username = user;
        this.password = password;
        this.useTLS = false;
    }

    @Override
    public long insertData(String data) {
        c Conn = getDefaultConnection();

        String[] rows = data.split(TSBM.LINE_SEPARATOR);
        StringBuilder sc = new StringBuilder();
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        int turn = 0;
        long costTime = 0L;
        String[] columnNames = new String[]{"time", "farmId", "deviceId", "sensorName", "sValue"}; //表头
        LinkedList<Timestamp> dateList = new LinkedList<>();
        LinkedList<String> farmIdList = new LinkedList<>();
        LinkedList<String> deviceIdList = new LinkedList<>();
        LinkedList<String> sensorNameList = new LinkedList<>();
        LinkedList<Double> valueList = new LinkedList<>();
        for (int i = 0; i < rows.length; i++) {
            String[] sensors = rows[i].split(TSBM.SEPARATOR);
            if (sensors.length < 3) {//过滤空行  边界问题？
                continue;
            }
            for (int index = 3; index < sensors.length; index++) {

                dateList.add(new Timestamp(Long.valueOf(sensors[0]))); // sensors[0] : timestamp
                farmIdList.add(sensors[1]); // sensors[1] : farmId
                deviceIdList.add(sensors[2]); // sensors[2] : deviceId
                sensorNameList.add("s" + (index - 2)); // "s" + (index - 2) : sensorName
                valueList.add(Double.valueOf(sensors[index])); // sensors[index] : value
            }
            turn++;
            if (turn == 10 || i == rows.length - 1) {  // 如果行数不为10倍数 到了最后一行就都塞进去
                turn = 0;
                //列式存储
                Object[] times = dateList.toArray();
                Object[] farmIds = farmIdList.toArray();
                Object[] deviceIds = deviceIdList.toArray();
                Object[] sensorNames = sensorNameList.toArray();
                Object[] sValues = valueList.toArray();

                Object[] merge = new Object[]{times, farmIds, deviceIds, sensorNames, sValues};

                c.Dict dict = new c.Dict(columnNames, merge);
                c.Flip table = new c.Flip(dict);

                try {
                    long startTime = System.nanoTime();
                    Conn.k("insert", dbName, table);
                    long endTime = System.nanoTime();
                    costTime += (endTime - startTime) / 1000 / 1000;

                    //clear
                    dateList.clear();
                    farmIdList.clear();
                    deviceIdList.clear();
                    sensorNameList.clear();
                    valueList.clear();
                } catch (IOException | KException e) {
                    e.printStackTrace();
                }
            }
        }
        closeConnection(Conn);
        return costTime;
    }

    public long execQuery(String qSql) {
        c Conn = getDefaultConnection();
        long costTime = 0L;
        try {
            long startTime = System.nanoTime();
            Conn.k(qSql);
            long endTime = System.nanoTime();
            costTime = endTime - startTime;
        } catch (IOException | KException e) {
            e.printStackTrace();
        }
        closeConnection(Conn);
        return costTime / 1000 / 1000;
    }

    @Override
    public long query1(long start, long end) {
        String[] starts = new Timestamp(start).toString().split(" ");
        String[] ends = new Timestamp(start).toString().split(" ");
        String startTime = starts[0].replace('-', '.') + "D" + starts[1];
        String endTime = ends[0].replace('-', '.') + "D" + ends[1];

        String sqlFormat = "select from %s where farmId=`%s,deviceId=`%s,sensorName=`%s,time >= %s,time <= %s";
        String qSql = String.format(sqlFormat, dbName, "f1", "d1", "s1", startTime, endTime);

        return execQuery(qSql);
    }

    @Override
    public long query2(long start, long end, double value) {
        String[] starts = new Timestamp(start).toString().split(" ");
        String[] ends = new Timestamp(start).toString().split(" ");
        String startTime = starts[0].replace('-', '.') + "D" + starts[1];
        String endTime = ends[0].replace('-', '.') + "D" + ends[1];

        String sqlFormat = "select from %s where farmId=`%s,deviceId=`%s,sValue >= %s,time >= %s,time <= %s";
        String qSql = String.format(sqlFormat, dbName, "f1", "d2", value, startTime, endTime);

        return execQuery(qSql);
    }

    @Override
    public long query3(long start, long end) {
        String[] starts = new Timestamp(start).toString().split(" ");
        String[] ends = new Timestamp(start).toString().split(" ");
        String startTime = starts[0].replace('-', '.') + "D" + starts[1];
        String endTime = ends[0].replace('-', '.') + "D" + ends[1];

        String sqlFormat = "select avgValue:avg(sValue) by farmId,deviceId,time(1h) from %s " +
                "where farmId=`%s,deviceId=`%s,sensorName=`%s,time >= %s,time <= %s";

        String qSql = String.format(sqlFormat, dbName, "f1", "d2", "s1",
                startTime, endTime);
        return execQuery(qSql);
    }

    @Override
    public long query4(long start, long end) {
        String[] starts = new Timestamp(start).toString().split(" ");
        String[] ends = new Timestamp(start).toString().split(" ");
        String startTime = starts[0].replace('-', '.') + "D" + starts[1];
        String endTime = ends[0].replace('-', '.') + "D" + ends[1];

        String sqlFormat = "`deviceId xasc select from %s where farmId=`%s, " +
                "sensorName in (`%s,`%s,`%s,`%s,`%s),time >= %s,time <= %s";
        String qSql = String.format(sqlFormat, dbName, "f1", "s1", "s2", "s3", "s4", "s5", startTime, endTime);
        return execQuery(qSql);
    }

    @Override
    public long query5(long start, long end) {
        String[] starts = new Timestamp(start).toString().split(" ");
        String[] ends = new Timestamp(start).toString().split(" ");
        String startTime = starts[0].replace('-', '.') + "D" + starts[1];
        String endTime = ends[0].replace('-', '.') + "D" + ends[1];

        String sqlFormat = "select from %s where farmId=`%s,time >= %s,time <= %s";
        String qSql = String.format(sqlFormat, dbName, "f1", startTime, endTime);
        return execQuery(qSql);
    }
}

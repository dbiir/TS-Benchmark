package cn.edu.ruc;

import cn.edu.ruc.adapter.BaseAdapter;
import cn.edu.ruc.start.TSBM;
import com.taosdata.jdbc.TSDBDriver;

import java.sql.*;
import java.util.Properties;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TdengineAdapter2 implements BaseAdapter {

    private Connection connection = null;
    private String url = null;
    private String ip = null;
    private String port = null;

    public Connection getConnection() {
        if (connection != null) {
            return connection;
        }
        this.url = String.format("jdbc:TAOS://%s:%s/test?user=root&password=taosdata", ip, port);
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            Properties connProps = new Properties();
            connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
            connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
            connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
            connection = DriverManager.getConnection(url, connProps);
            if (connection == null) {
                return null;
            }
            return connection;
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
            connection = null;
            return null;
        }
    }

    @Override
    public void initConnect(String ip, String port, String user, String password) {
        this.ip = ip;
        this.port = port;
        connection = getConnection();
        // 创建数据库
        try {
            Statement stm = connection.createStatement();
            stm.executeUpdate("create database if not exists test");
            stm.executeUpdate("use test");
            // 创建超级表
            stm.executeUpdate("create stable metrics (time timestamp, value float) TAGS (farm nchar(6), device nchar(6), s nchar(4))");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return;
    }

    @Override
    public long insertData(String data) {
        String[] rows = data.split(TSBM.LINE_SEPARATOR);
        StringBuffer sqls = new StringBuffer();
        String sqlFormat = "%s USING metrics TAGS (\"%s\",\"%s\",\"%s\") VALUES (%s) ";
        int count = 0;
        long costTime = 0L;
        for (String row : rows) {
            String[] sensors = row.split(TSBM.SEPARATOR);
            if (sensors.length < 3) {//过滤空行
                continue;
            }
            String timestamp = sensors[0];
            String farmId = sensors[1];
            String deviceId = sensors[2];

            StringBuffer values = new StringBuffer();
            int length = sensors.length;
            for (int index = 3; index < length; index++) {
                String sensorName = "s" + (index - 2);
                String value = sensors[index];
                String tbname = String.format("%s%s%s", farmId, deviceId,sensorName);
                values.append(timestamp);
                values.append(",");
                values.append(value);
                sqls.append(String.format(sqlFormat, tbname, farmId, deviceId, sensorName, values.toString()));
                sqls.append("\t");
                values.setLength(0);
            }
            count++;
            if (count == 10) {
                count = 0;
                StringBuffer stringBuffer = new StringBuffer();
                //String str = sqls.toString();
                //System.out.println(str);
                String SQL = stringBuffer.append("INSERT INTO ").append(sqls.toString()).toString();
                long startTime = System.nanoTime();
                connection = getConnection();
                if (connection != null) {
                    try {
                        Statement stmt = connection.createStatement();
                        stmt.execute(SQL);
                        sqls.setLength(0);
                        stmt.close();
                        long endTime = System.nanoTime();
                        costTime += (endTime - startTime)/1000/1000;
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
            }
        }
        if (sqls.length() != 0) {
            sqls.append(";");
            StringBuffer stringBuffer = new StringBuffer();
            String SQL = stringBuffer.append("INSERT INTO ").append(sqls.toString()).toString();
            if (connection != null) {
                try {
                    long startTime = System.nanoTime();
                    Statement stmt = connection.createStatement();
                    stmt.execute(SQL);
                    stmt.close();
                    long endTime = System.nanoTime();
                    costTime += (endTime - startTime)/1000/1000;
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
        return costTime;
    }

    private void closeStatement(Statement statement) {
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public long execQuery(String sql) {
        connection = getConnection();
        Statement statement = null;
        long costTime = 0;
        try {
            statement = connection.createStatement();
            long startTime = System.nanoTime();
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            long endTime = System.nanoTime();
            costTime = endTime - startTime;
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        } finally {
            closeStatement(statement);
        }
        return costTime / 1000 / 1000;
    }

    @Override
    public long query1(long start, long end) {
        String sqlFormat = "select time,farm,device,s from metrics where farm='%s' and device='%s' and s='%s' and time>=\"%s\" " +
                "and time<=\"%s\"";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String start_time = sdf.format(new Date(Long.parseLong(String.valueOf(start))));
        String end_time = sdf.format(new Date(Long.parseLong(String.valueOf(end))));
        String eSql = String.format(sqlFormat, "f1", "d1", "s1", start_time, end_time);
        System.out.println(eSql);
        return execQuery(eSql);
        //return 1;
    }

    @Override
    public long query2(long start, long end, double value) {
        String sqlFormat = "select time,farm,device,s from metrics where farm='%s' and s='%s' and vale>%s " +
                "and time>\"%s\" and time< \"%s\"";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String start_time = sdf.format(new Date(Long.parseLong(String.valueOf(start))));
        String end_time = sdf.format(new Date(Long.parseLong(String.valueOf(end))));

        String eSql = String.format(sqlFormat, "f1","s1",value, start_time, end_time);
        System.out.println(eSql);
        return execQuery(eSql);
        //return 1;
    }

    @Override
    public long query3(long start, long end) {
        String sqlFormat = "select avg(value) " +
                "from metrics " +
                "where farm='%s' and  time>\"%s\" and time<\"%s\"" +
                " INTERVAL(1h)" +
                " group by farm,device,s";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String start_time = sdf.format(new Date(Long.parseLong(String.valueOf(start))));
        String end_time = sdf.format(new Date(Long.parseLong(String.valueOf(end))));
        String eSql = String.format(sqlFormat, "f1", start_time, end_time);
        System.out.println(eSql);
        return execQuery(eSql);
    }

    @Override
    public long query4(long start, long end) {

        String sqlFormat = "select * from metrics where f='%s' and (s='%s' or s='%s' or s='%s' or s='%s' or s='%s') " +
                "and time>=%s and time<=%s";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String start_time = sdf.format(new Date(Long.parseLong(String.valueOf(start))));
        String end_time = sdf.format(new Date(Long.parseLong(String.valueOf(end))));
        String eSql = String.format(sqlFormat,"f1","s1", "s2", "s3", "s4", "s5", start_time, end_time);

        return execQuery(eSql);
    }

    @Override
    public long query5(long start, long end) {
        String sqlFormat = "select * from metrics where farm='%s' and time>\"%s\" and time<\"%s\"";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String start_time = sdf.format(new Date(Long.parseLong(String.valueOf(start))));
        String end_time = sdf.format(new Date(Long.parseLong(String.valueOf(end))));

        String eSql = String.format(sqlFormat, "f1", start_time, end_time);
        System.out.println(eSql);
        return execQuery(eSql);
    }
}

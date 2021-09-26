package cn.edu.ruc;

import cn.edu.ruc.adapter.BaseAdapter;
import cn.edu.ruc.start.TSBM;
import com.taosdata.jdbc.TSDBDriver;

import java.sql.*;
import java.util.Properties;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TdengineAdapter implements BaseAdapter {

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
            stm.executeUpdate("create stable metrics (time timestamp, s1 float, s2 float, s3 float, s4 float, s5 float, s6 float, s7 float, s8 float, s9 float, s10 float, s11 float, s12 float, s13 float, s14 float, s15 float, s16 float, s17 float, s18 float, s19 float, s20 float, s21 float, s22 float, s23 float, s24 float, s25 float, s26 float, s27 float, s28 float, s29 float, s30 float, s31 float, s32 float, s33 float, s34 float, s35 float, s36 float, s37 float, s38 float, s39 float, s40 float, s41 float, s42 float, s43 float, s44 float, s45 float, s46 float, s47 float, s48 float, s49 float, s50 float) TAGS (farm nchar(6), device nchar(6))");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return;
    }

    @Override
    public long insertData(String data) {
        String[] rows = data.split(TSBM.LINE_SEPARATOR);
        StringBuffer sqls = new StringBuffer();
        String sqlFormat = "%s USING metrics TAGS (\"%s\",\"%s\") VALUES (%s) ";
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

            String tbname = String.format("%s%s", farmId, deviceId);

            StringBuffer values = new StringBuffer();
            values.append(timestamp);
            values.append(",");
            int length = sensors.length;
            for (int index = 3; index < length; index++) {
                String value = sensors[index];
                values.append(value);
                values.append(",");
            }
            String vs = values.toString();
            sqls.append(String.format(sqlFormat, tbname, farmId, deviceId, vs.substring(0, vs.length() - 1)));
            sqls.append("\t");
            count++;
            if (count == 10) {
                count = 0;
                StringBuffer stringBuffer = new StringBuffer();
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
        String sqlFormat = "select time,farm,device,%s from metrics where farm='%s' and device='%s' and time>=\"%s\" " +
                "and time<=\"%s\"";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String start_time = sdf.format(new Date(Long.parseLong(String.valueOf(start))));
        String end_time = sdf.format(new Date(Long.parseLong(String.valueOf(end))));
        String eSql = String.format(sqlFormat, "s1", "f1", "d1", start_time, end_time);
        System.out.println(eSql);
        return execQuery(eSql);
        //return 1;
    }

    @Override
    public long query2(long start, long end, double value) {
        String sqlFormat = "select time,farm,device,%s from metrics where farm='%s' and s1>%s " +
                "and time>\"%s\" and time< \"%s\"";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String start_time = sdf.format(new Date(Long.parseLong(String.valueOf(start))));
        String end_time = sdf.format(new Date(Long.parseLong(String.valueOf(end))));

        String eSql = String.format(sqlFormat, "s1", "f1", value, start_time, end_time);
        System.out.println(eSql);
        return execQuery(eSql);
        //return 1;
    }

    @Override
    public long query3(long start, long end) {
        String sqlFormat = "select %s " +
                "from metrics " +
                "where farm='%s' and  time>\"%s\" and time<\"%s\"" +
                " INTERVAL(1h)" +
                " group by farm,device";
        StringBuffer columnsBuffer = new StringBuffer();
        for (int index = 1; index <= 50; index++) {
            columnsBuffer.append("avg(");
            columnsBuffer.append("s");
            columnsBuffer.append(index);
            columnsBuffer.append(")");
            if (index != 50) {
                columnsBuffer.append(",");
            }
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String start_time = sdf.format(new Date(Long.parseLong(String.valueOf(start))));
        String end_time = sdf.format(new Date(Long.parseLong(String.valueOf(end))));
        String eSql = String.format(sqlFormat, columnsBuffer.toString(), "f1", start_time, end_time);
        System.out.println(eSql);
        return execQuery(eSql);
        //return 1;
    }

    @Override
    public long query4(long start, long end) {
        //select time,f,d,s1,s2,s3,s4,s5 from sensor;
        String sqlFormat = "select time,farm,device,%s,%s,%s,%s,%s from metrics " +
                "where farm='%s' and  time>\"%s\" and time<\"%s\"";

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String start_time = sdf.format(new Date(Long.parseLong(String.valueOf(start))));
        String end_time = sdf.format(new Date(Long.parseLong(String.valueOf(end))));

        String eSql = String.format(sqlFormat, "s1", "s2", "s3", "s4", "s5", "f1", start_time, end_time);
        System.out.println(eSql);
        return execQuery(eSql);
        //return 1;
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
        //return 1;
    }
}

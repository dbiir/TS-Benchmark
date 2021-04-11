package cn.edu.ruc;

import cn.edu.ruc.adapter.BaseAdapter;
import cn.edu.ruc.start.TSBM;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class TimescaledbAdapter implements BaseAdapter {
    private String driverClass = "org.postgresql.Driver";
    private String url = "";
    private String user = "";
    private String passwd = "";
    private String dbName = "ruc_test";

    public void initConnect(String ip, String port, String user, String password) {
        this.url = String.format("jdbc:postgresql://%s:%s/%s", ip, port, this.dbName);
        try {
            Class.forName(driverClass);
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.user = user;
        this.passwd = password;
    }

    private Connection getConnection() {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url, user, passwd);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    private void closeConnection(Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
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

    public long insertData(String data) {
        String[] rows = data.split(TSBM.LINE_SEPARATOR);
        StringBuilder sc = new StringBuilder();
        List<String> sqls = new ArrayList<String>();
        for (String row : rows) {
            String sqlFormat = "insert into sensor(%s) values(%s)";
            String[] sensors = row.split(TSBM.SEPARATOR);
            if (sensors.length < 3) {//过滤空行
                continue;
            }

            String timestamp = sensors[0];
            String farmId = sensors[1];
            String deviceId = sensors[2];
            int length = sensors.length;
            StringBuffer tagBuffer = new StringBuffer();
            StringBuffer valueBuffer = new StringBuffer();
            tagBuffer.append("time");
            tagBuffer.append(",");
            tagBuffer.append("f");
            tagBuffer.append(",");
            tagBuffer.append("d");
            tagBuffer.append(",");
            valueBuffer.append("to_timestamp(");
            valueBuffer.append((Long.parseLong(timestamp) / 1000) + "");
            valueBuffer.append(")");
            valueBuffer.append(",'");
            valueBuffer.append(farmId);
            valueBuffer.append("','");
            valueBuffer.append(deviceId);
            valueBuffer.append("',");
            for (int index = 3; index < length; index++) {
                String value = sensors[index];
                String sensorName = "s" + (index - 2);
                tagBuffer.append(sensorName);
                valueBuffer.append(value);
                if (index != length - 1) {
                    tagBuffer.append(",");
                    valueBuffer.append(",");
                }
            }
            sqls.add(String.format(sqlFormat, tagBuffer.toString(), valueBuffer.toString()));
        }
        // 写入数据
        Connection connection = getConnection();
        Statement statement = null;
        Long costTime = 0L;
        try {
            statement = connection.createStatement();
            for (String sql : sqls) {
                statement.addBatch(sql);
            }
            long startTime = System.nanoTime();
            statement.executeBatch();
            long endTime = System.nanoTime();
            costTime = endTime - startTime;
            statement.clearBatch();
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        } finally {
            closeStatement(statement);
            closeConnection(connection);
        }
        return costTime / 1000 / 1000;
    }

    public long query1(long start, long end) {
        String sqlFormat = "select time,f,d,%s from sensor where f='%s' and d='%s' and time>=to_timestamp(%s) " +
                "and time<=to_timestamp(%s)";
        String eSql = String.format(sqlFormat, "s1", "f1", "d1", start / 1000, end / 1000);
        System.out.println(eSql);
        return execQuery(eSql);
    }

    public long query2(long start, long end, double value) {
        String sqlFormat = "select time,f,d,%s from sensor where f='%s' and s1>%s " +
                "and time>to_timestamp(%s) and time<to_timestamp(%s)";
        String eSql = String.format(sqlFormat, "s1", "f1", value, start / 1000, end / 1000);
        return execQuery(eSql);
    }

    public long query3(long start, long end) {
        //select time_bucket('1 hour', time) AS one_hour,f,d, avg(s1),avg(s2) from sensor group by fifteen_min,f,d;
        String sqlFormat = "select time_bucket('1 hour', time) AS one_hour,f,d,%s " +
                "from sensor where f='%s' and  time>to_timestamp(%s) and time<to_timestamp(%s)" +
                " group by one_hour,f,d";
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
        String eSql = String.format(sqlFormat, columnsBuffer.toString(), "f1", start / 1000, end / 1000);
        return execQuery(eSql);
    }

    public long query4(long start, long end) {
        //select time,f,d,s1,s2,s3,s4,s5 from sensor;
        String sqlFormat = "select time,f,d,%s,%s,%s,%s,%s from sensor " +
                "where f='%s' and  time>to_timestamp(%s) and time<to_timestamp(%s)";
        String eSql = String.format(sqlFormat, "s1", "s2", "s3", "s4", "s5", "f1", start / 1000, end / 1000);
        return execQuery(eSql);
    }

    public long query5(long start, long end) {
        //select * from sensor;
        String sqlFormat = "select * from sensor " +
                "where f='%s' and time>to_timestamp(%s) and time<to_timestamp(%s)";
        String eSql = String.format(sqlFormat, "f1", start / 1000, end / 1000);
        return execQuery(eSql);
    }

    public long execQuery(String sql) {
        Connection conn = getConnection();
        Statement statement = null;
        long costTime = 0;
        try {
            statement = conn.createStatement();
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
            closeConnection(conn);
        }
        return costTime / 1000 / 1000;
    }
}

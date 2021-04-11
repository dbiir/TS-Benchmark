package cn.edu.ruc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TimescaledbUtils {
    public static void main(String[] args) {
        initTables("127.0.0.1");
    }

    public static void initTables(String ip) {
        String driverClass = "org.postgresql.Driver";
        try {
            Class.forName(driverClass);
        } catch (Exception e) {
            e.printStackTrace();
        }
        String userName = "postgres";
        String passwd = "postgres";
        String url = "jdbc:postgresql://" + ip + ":5432/ruc_test";

        int sNum = 50;
        Connection connection = null;
        Statement statement = null;
        try {
            connection = getConnection(url, userName, passwd);
            statement = connection.createStatement();
            String dropTableSql = "drop table if  exists sensor";
            statement.execute(dropTableSql);
            String createTableFormat = "create table if not exists sensor(time TIMESTAMPTZ NOT NULL," +
                    "f varchar(128) NOT NULL,d varchar(128) NOT NULL,%s)";
            StringBuffer valueTagBuffer = new StringBuffer();
            for (int sensorIdx = 1; sensorIdx <= sNum; sensorIdx++) {
                String sensorCode = "s" + sensorIdx;
                valueTagBuffer.append(sensorCode);
                valueTagBuffer.append(" ");
                valueTagBuffer.append(" double  PRECISION");
                if (sensorIdx != sNum) {
                    valueTagBuffer.append(",");
                }
            }
            String createTableSql = String.format(createTableFormat, valueTagBuffer.toString());
            System.out.println(createTableSql);
            statement.execute(createTableSql);
            String addIndexSql1 = "CREATE INDEX ON sensor (f)";
            String addIndexSql2 = "CREATE INDEX ON sensor (d)";
            statement.execute(addIndexSql1);
            statement.execute(addIndexSql2);
            String createHyperTableSql = "SELECT create_hypertable('sensor', 'time')";
            statement.execute(createHyperTableSql);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeStatement(statement);
            closeConnection(connection);
        }

    }

    private static Connection getConnection(String url, String userName, String passwd) {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url, userName, passwd);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    private static void closeConnection(Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void closeStatement(Statement statement) {
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

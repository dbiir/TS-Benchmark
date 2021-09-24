package cn.edu.ruc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class IotDBUtils {
    public static void main(String[] args) {
        initTimeseries("192.168.151.51");
    }

    public static void initTimeseries(String ip) {
        String driverClass = "org.apache.iotdb.jdbc.IoTDBDriver";
        try {
            Class.forName(driverClass);
        } catch (Exception e) {
            e.printStackTrace();
        }
        String userName = "root";
        String passwd = "root";
        String rootSeries = "root.p";
        String url = "jdbc:iotdb://" + ip + ":6667/";

        int farmNum = 64;
        int dNum = 300;
        int sNum = 50;
        Connection connection = null;
        Statement statement = null;
        try {
            connection = getConnection(url, userName, passwd);
            statement = connection.createStatement();
            try {
                String setStorageSql = null;
                try {
                    setStorageSql = "SET STORAGE GROUP TO " + rootSeries;
                    statement.execute(setStorageSql);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                for (int farmIdx = 1; farmIdx <= farmNum; farmIdx++) {
                    String farmCode = "f" + farmIdx;
                    for (int deviceIdx = 1; deviceIdx <= dNum; deviceIdx++) {
                        try {
                            String deviceCode = "d" + deviceIdx;
                            for (int sensorIdx = 1; sensorIdx <= sNum; sensorIdx++) {
                                String sensorCode = "s" + sensorIdx;
                                String sql = "CREATE TIMESERIES " + rootSeries + "." + farmCode + "." + deviceCode + "." + sensorCode + "  WITH DATATYPE=FLOAT, ENCODING=RLE";
                                statement.addBatch(sql);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    System.out.println("farm_idx:" + farmIdx);
                    try {
                        statement.executeBatch();
                        statement.clearBatch();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
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

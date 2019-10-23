package cn.edu.ruc;

import cn.edu.ruc.adapter.BaseAdapter;
import cn.edu.ruc.start.TSBM;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * iotdb test
 * 优缺点，无法自动创建时间序列
 */
public class IotdbAdapter implements BaseAdapter {
    private String driverClass="cn.edu.tsinghua.iotdb.jdbc.TsfileDriver";
    private String userName="";
    private String passwd="";
    private String rootSeries="root.p";
    private String url="";
    public void initConnect(String ip, String port, String user, String password) {
        try {
            Class.forName(driverClass);
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.url=String.format("jdbc:tsfile://%s:%s/",ip,port);
        this.userName=user;
        this.passwd=password;
    }
    private Connection getConnection(){
        Connection connection=null;
        try {
            connection = DriverManager.getConnection(url, userName, passwd);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }
    private void closeConnection(Connection conn){
        try {
            if(conn!=null){
                conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private void closeStatement(Statement statement){
        try {
            if(statement!=null){
                statement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public long insertData(String data) {
        String[] rows = data.split(TSBM.LINE_SEPARATOR);
        StringBuilder sc = new StringBuilder();
        List<String> sqls=new ArrayList<String>();
        for (String row : rows) {
            String sqlFormat="insert into %s.%s.%s(%s) values(%s)";
            String[] sensors = row.split(TSBM.SEPARATOR);
            if (sensors.length < 3) {//过滤空行
                continue;
            }
            String timestamp = sensors[0];
            String farmId = sensors[1];
            String deviceId = sensors[2];
            int length = sensors.length;
            StringBuffer tagBuffer=new StringBuffer();
            StringBuffer valueBuffer=new StringBuffer();
            tagBuffer.append("timestamp");
            tagBuffer.append(",");
            valueBuffer.append(timestamp);
            valueBuffer.append(",");
            for (int index = 3; index < length; index++) {
                String value = sensors[index];
                String sensorName = "s" + (index - 2);
                tagBuffer.append(sensorName);
                valueBuffer.append(value);
                if(index!=length-1){
                    tagBuffer.append(",");
                    valueBuffer.append(",");
                }
            }
            sqls.add(String.format(sqlFormat,rootSeries,farmId,deviceId,tagBuffer.toString(),valueBuffer.toString()));
        }
        // 写入数据
        Connection connection = getConnection();
        Statement statement = null;
        Long costTime=0L;
        try {
            statement = connection.createStatement();
            for(String sql:sqls) {
                statement.addBatch(sql);
            }
            long startTime=System.nanoTime();
            int[] ints = statement.executeBatch();
            for(int i:ints){
                System.out.println(i);
            }
            long endTime=System.nanoTime();
            costTime=endTime-startTime;
            statement.clearBatch();
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }finally {
            closeStatement(statement);
            closeConnection(connection);
        }
        return costTime/1000/1000;
    }

    public long query1(long start, long end) {
        String formatSql="select %s from  %s.%s.%s  where time>=%s and time<=%s";
        String eSql=String.format(formatSql,"s1",rootSeries,"f1","d1",start,end);
        System.out.println(eSql);
        return execQuery(eSql);
    }

    public long query2(long start, long end, double value) {
        //select s1 from root.perform.f1.* where s1>0;
        String formatSql="select %s from  %s.%s.*  where time>=%s and time<=%s and %s>%s";
        String eSql=String.format(formatSql,"s1",rootSeries,"f1",start,end,"s1",value);
        System.out.println(eSql);
        return execQuery(eSql);
    }

    public long query3(long start, long end) {
        //select mean(*) from root.perform.f1.* group by (1h,[0,100000000]);
        String formatSql="select mean(s1) from  %s.%s.* where time>=%s and time<=%s group by " +
                "(1h,[%s,%s])";
        String eSql=String.format(formatSql,rootSeries,"f1",start,end,start,end);
        System.out.println(eSql);
        return execQuery(eSql);
    }

    public long query4(long start, long end) {
        //select s1,s2 from root.perform.f1.* ;
        String formatSql="select %s,%s,%s,%s,%s from %s.%s.* where time>=%s and time<=%s";
        String eSql=String.format(formatSql,"s1","s2","s3","s4","s5",rootSeries,"f1",start,end);
        System.out.println(eSql);
        return execQuery(eSql);
    }

    public long query5(long start, long end) {
        //select * from root.perform.f1.* ;
        String formatSql="select * from %s.%s.* where time>=%s and time<=%s";
        String eSql=String.format(formatSql,rootSeries,"f1",start,end);
        System.out.println(eSql);
        return execQuery(eSql);
    }
    public long execQuery(String sql) {
        Connection conn=getConnection();
        Statement statement=null;
        long costTime=0;
        try {
            statement=conn.createStatement();
            long startTime=System.nanoTime();
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            long endTime=System.nanoTime();
            costTime=endTime-startTime;
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        }finally{
            closeStatement(statement);
            closeConnection(conn);
        }
        return costTime/1000/1000;
    }
}

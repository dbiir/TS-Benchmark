package cn.edu.ruc.adapter;

/**
 * modified by rainmaple
 * adapter base interface
 * 适配器基础类
 */
public interface BaseAdapter {
    public void initConnect(String ip, String port, String user, String password);
    /**
     * @return timeout ,if request failed, please return -1;
     */
    public long insertData(String data);

    /**
     * the method query1
     * select * from table where f_id=f and d_id=d and s_id=s and time>start and time<end
     * @param start query data  start time
     * @param end query data end time
     * @return
     */
    public long query1(long start, long end);
    /**
     * the method query2
     * select time,s_id,d_id,f_id from table where f_id=f and s_id=s and value>X and time>start and time<end (1 week)
     * select * from table where f_id=f and d_id=d_id and s_id=s_id and time>time-15min and time <time+15min
     * @param start query data  start time
     * @param end query data end time
     * @return
     */
    public long query2(long start, long end, double value);
    /**
     * the method query3
     * select f_id, d_id , avg(value) from table where f_id=f and time>start and time<end group by f_id,d_id, hour (1 week)
     * @param start query data  start time
     * @param end query data end time
     * @return
     */
    public long query3(long start, long end);
    /**
     * the method query4
     * select * from table where f_id=f and s_id in (s1,s2,s3,s4,s5) time>start and time <end
     * @param start query data  start time
     * @param end query data end time
     * @return
     */
    public long query4(long start, long end);
    /**
     * the method query5
     * select * from table where f_id=f and time>start and time<end
     * @param start query data  start time
     * @param end query data end time
     * @return
     */
    public long query5(long start, long end);
}

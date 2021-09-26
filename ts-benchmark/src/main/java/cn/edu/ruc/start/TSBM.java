package cn.edu.ruc.start;

import cn.edu.ruc.adapter.BaseAdapter;
import cn.edu.ruc.utils.FileUtils;
import cn.edu.ruc.utils.ValueUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;

/**
 * 入口类 BootStrap
 */
public class TSBM {
    public static final String SEPARATOR = ",";
    public static final String LINE_SEPARATOR = System.getProperty("line.separator");
    private static final Random RANDOM = new Random();
    private static final long SLEEP_TIME = 200L;
    private static final int MAX_FARM = 512;
    private static final int MAX_ROWS = 300;
    private static final int MAX_SENSOR = 50;
    private static final int SUM_FARM = 2;
    private static final long IMPORT_START = 1514736000000L;
    private static BlockingQueue<String> queue = new LinkedBlockingQueue<String>(100);

    public static void main(String[] args) throws Exception {
    }

    /**
     * start to test
     *
     * @param basePath  the data path that the benchmark in runing generate
     * @param className database adapter
     * @param ip
     * @param port
     * @param userName
     * @param password
     */
    public static void startPerformTest(String basePath, String className, String ip, String port, String userName,
                                        String password) {
        startPerformTest(basePath, className, ip, port, userName, password, true, true);
    }

    /**
     * start to test
     *
     * @param basePath
     * @param className
     * @param ip
     * @param port
     * @param userName
     * @param password
     * @param generateParam whether to generate disk data,true means  generate,false is not ;
     * @param loadParam     whether to load data to database,true means  load,false is not ;
     */
    public static void startPerformTest(String basePath, String className, String ip, String port, String userName,
                                        String password, boolean generateParam, boolean loadParam) {
        BaseAdapter adapter = null;
        try {
            adapter = (BaseAdapter) Class.forName(className).newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
        adapter.initConnect(ip, port, userName, password);
        String dataPath = basePath + "/data/";
        String resultPath = basePath + "/result/";
        String resultFile = resultPath + "/" + System.currentTimeMillis() + ".txt";
        int maxFarm = MAX_FARM;
        int maxRows = MAX_ROWS;
        // 1 数据生成
        if (generateParam) {
            System.out.println(">>>>>>>>>>generate data begin " + System.currentTimeMillis() + ">>>>>>>>>>");
            generateDiskData(dataPath, maxFarm, maxRows);
            System.out.println("<<<<<<<<<<generate data finished " + System.currentTimeMillis() + "<<<<<<<");
        } else {
            System.out.println(">>>>>>>>>>generate insert data begin " + System.currentTimeMillis() + ">>>>>>>>>>");
//            generateInsertData(dataPath, maxFarm, maxRows);
            System.out.println("<<<<<<<<<<generate insert data finished " + System.currentTimeMillis() + "<<<<<<<");
        }

        // 2 导入
        if (loadParam) {
            System.out.println(">>>>>>>>>>load data begin " + System.currentTimeMillis() + ">>>>>>>>>>>>>>");
            String importResult = importData(adapter, dataPath);
            System.out.println(importResult);
            writeResult(resultFile, importResult);
            System.out.println("<<<<<<<<<<load data finished " + System.currentTimeMillis() + "<<<<<<<<<<<");

        }

        // 3 append测试
        System.out.println(">>>>>>>>>>append test begin " + System.currentTimeMillis() + ">>>>>>>>>>>>");
        String appendResult = appendPerform(dataPath, adapter, maxFarm, maxRows);
        System.out.println(appendResult);
        writeResult(resultFile, appendResult);
        System.out.println("<<<<<<<<<<append test finished " + System.currentTimeMillis() + "<<<<<<<<<<<<<<");

        // 4 query测试
        System.out.println(">>>>>>>>>>query test begin>>>>>>>>>>>>>>");
        String readResult = readPerform(adapter);
        System.out.println(readResult);
        writeResult(resultFile, readResult);
        System.out.println("<<<<<<<<<<query end finished<<<<<<<<<<<<<<");
        // 输出结果
        System.out.println("test finished");
        System.out.println("test result in file " + resultFile);
    }

    /**
     * the method is used to generate disk data.
     *
     * @param basePath the path of saving data
     */
    public static void generateData(String basePath) {
        String dataPath = basePath + "/data/";
        System.out.println(">>>>>>>>>>generate data begin " + System.currentTimeMillis() + ">>>>>>>>>>");
        generateDiskData(dataPath, MAX_FARM, MAX_ROWS);
        System.out.println("<<<<<<<<<<generate data finished " + System.currentTimeMillis() + "<<<<<<<");
    }
///////////////////////////////////////////////////数据生成

    /**
     * 生成磁盘数据
     *
     * @param basePath 存储数据的路径
     * @param maxFarm  最大farm数
     * @param maxRows  每个farm对应的最大设备数
     */
    private static void generateDiskData(String basePath, int maxFarm, int maxRows) {
        generateInsertData(basePath, maxFarm, maxRows);
    }

    private static void generateInsertData(String basePath, int maxFarm, int maxRows) {

        long importEnd = IMPORT_START + 12 * 3600 * 1000;
        // 2 生成 1/2/4/8/16/32/64 farm数据 每个farm50个device，每个10批次，一个批次一个文件
        int batchSum = 5;
        long insertStart = importEnd;
        for (int farmNum = 1; farmNum <= maxFarm; farmNum = farmNum * 2) {
            int deviceNum = 50;
            for (int batchNum = 1; batchNum <= batchSum; batchNum++) {
                for (int cFarm = 1; cFarm <= farmNum; cFarm++) {
                    String path = basePath + "/farm/" + farmNum + "/" + batchNum + "/" + cFarm;
                    FileUtils.writeLine(path, generateData(insertStart + 7000 * (batchNum - 1),
                            insertStart + 7000 * batchNum, cFarm, 50));
                }
            }
            insertStart += 7000 * batchSum * farmNum;
        }
        // 3 生成 8个farm，farm50，100，150，200，250，300数据
        for (int rowNum = 50; rowNum <= maxRows; rowNum = rowNum + 50) {
            int farmNum = 8;
            for (int batchNum = 1; batchNum <= batchSum; batchNum++) {
                for (int cFarm = 1; cFarm <= farmNum; cFarm++) {
                    String path = basePath + "/device/" + rowNum + "/" + batchNum + "/" + cFarm;
                    FileUtils.writeLine(path, generateData(insertStart + 7000 * (batchNum - 1)
                            , insertStart + 7000 * batchNum, cFarm, rowNum));
                }
            }
            insertStart += 7000 * batchSum * farmNum;
        }
    }

    private static String generateData(long start, long end, long farmId, long rows) {
        int step = 7000;
        int sumSensor = MAX_SENSOR;
        StringBuffer dataBuffer = new StringBuffer();
        for (; start < end; start += step) {
            for (int rowIndex = 1; rowIndex <= rows; rowIndex++) {
                StringBuffer dBuffer = new StringBuffer();
                dBuffer.append(start);
                dBuffer.append(SEPARATOR);
                dBuffer.append("f" + farmId);
                dBuffer.append(SEPARATOR);
                dBuffer.append("d" + rowIndex);
                for (int sn = 1; sn <= sumSensor; sn++) {
                    dBuffer.append(SEPARATOR);
                    dBuffer.append(String.format("%.5f", ValueUtils.getValueByField((int) farmId, sn, start)));
                }
                dBuffer.append(LINE_SEPARATOR);
                dataBuffer.append(dBuffer.toString());
            }
        }
        return dataBuffer.toString();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //////////////////////数据导入////////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private static String importData(BaseAdapter adapter, String dataPath) {
        long importStart = IMPORT_START;// 2018-01-01 00:00:00
        long importEnd = IMPORT_START + 12 * 3600 * 1000;
        final Boolean[] out = {false};
        final long[] nums= {0L};
        Thread th1 = new Thread() {
            public void run() {
                int count = 0;
                long startTime = System.nanoTime();
                for (long start = importStart; start <= importEnd; start += 7000) {
                    long end = importEnd < start + 7000 ? importEnd : start + 7000;
                    int sumFarm = SUM_FARM;// 历史数据风场数
                    StringBuffer dataBuffer = new StringBuffer();
                    for (int farmId = 1; farmId <= sumFarm; farmId++) {
                        dataBuffer.append(generateData(start, end, farmId, 50));
                        count++;
                        if (count == 100) {
                            try {
                                queue.put(dataBuffer.toString());
                            }catch (InterruptedException e){
                                e.printStackTrace();
                            }
                            dataBuffer.setLength(0);
                            count = 0;
                            nums[0] += 1;
                            long endTime = System.nanoTime();
                            long costTime = (endTime - startTime) / 1000000000;
                            System.out.println("Insert Total " + nums[0] + "(25万点), Used Time :" + costTime + "s");
                        }
                    }
                }
                out[0] = true;
            }
        };

        Thread th2 = new Thread() {
            public void run() {
                while (true) {
                    if (out[0]) {
                        break;
                    }
                    try {
                        String data = queue.take();
                        if (data == null) {
                            Thread.sleep(100);
                            continue;
                        }
                        adapter.insertData(data);
                    }catch (InterruptedException e){
                        e.printStackTrace();
                    }
                }
            }
        };

        Thread th3 = new Thread() {
            public void run() {
                while (true) {
                    if (out[0]) {
                        break;
                    }
                    try {
                        String data = queue.take();
                        if (data == null) {
                            Thread.sleep(100);
                            continue;
                        }
                        adapter.insertData(data);
                    }catch (InterruptedException e){
                        e.printStackTrace();
                    }
                }
            }
        };
        try {
            th1.start();
            Thread.sleep(1000);
            th2.start();
            th3.start();
            th1.join();
            th2.join();
            th3.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return "";
    }

//////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////写入测试/////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * append test
     */
    private static String appendPerform(String basePath, BaseAdapter adapter, int maxFarm, int maxRows) {
        int sleepTime = 7000;
        StringBuffer appendResultBuffer = new StringBuffer();
        System.out.println(">>>>>>>>>>append-1 start " + System.currentTimeMillis() + ">>>>>>>>>>");
        appendResultBuffer.append("##append  result");
        appendResultBuffer.append(LINE_SEPARATOR);
        // farm++ test
        appendResultBuffer.append("###append farm++ result");
        appendResultBuffer.append(LINE_SEPARATOR);
        for (int farm = 1; farm <= maxFarm; farm = farm * 2) {
            int batchMax = 5;
            int row = 50;
            ExecutorService pool = Executors.newFixedThreadPool(farm);
            CompletionService<Long> cs = new ExecutorCompletionService<Long>(pool);
            long sumPps = 0L;
            //每个风场，每个7s发送一次数据
            Map<Integer, Integer> thinkTimeMap = new HashMap<Integer, Integer>();
            for (int cFarm = 1; cFarm <= farm; cFarm++) {
                int thinkTime = RANDOM.nextInt(sleepTime);
                thinkTimeMap.put(cFarm, thinkTime);
            }
            for (int batch = 1; batch <= batchMax; batch++) {
                long startTime = System.currentTimeMillis();
                for (int cFarm = 1; cFarm <= farm; cFarm++) {
                    String path = basePath + "/farm/" + farm + "/" + batch + "/" + cFarm;
                    executeAppend(adapter, cs, path, thinkTimeMap.get(cFarm));
                }
                long pps = calcThroughtPut(row, farm, cs);
                sumPps += pps;
                System.out.println("append 1." + farm + "." + batch + " finished " + pps);
                long endTime = System.currentTimeMillis();
                long costTime = endTime - startTime;
                // 每七秒执行一次
                sleep(sleepTime - costTime > 0 ? sleepTime - costTime : 1);
            }
            appendResultBuffer.append("farm");
            appendResultBuffer.append("\t");
            appendResultBuffer.append(farm);
            appendResultBuffer.append("\t");
            appendResultBuffer.append(sumPps / batchMax);
            appendResultBuffer.append(LINE_SEPARATOR);
            pool.shutdown();
        }
        System.out.println(">>>>>>>>>>append-1 end " + System.currentTimeMillis() + ">>>>>>>>>>");
        System.out.println(">>>>>>>>>>append-2 start " + System.currentTimeMillis() + ">>>>>>>>>>");
        // row++ test
        appendResultBuffer.append("###append device++ result");
        appendResultBuffer.append(LINE_SEPARATOR);
        for (int row = 50; row <= maxRows; row = row + 50) {
            int batchMax = 5;
            int farm = 8;//线程数
            ExecutorService pool = Executors.newFixedThreadPool(farm);
            CompletionService<Long> cs = new ExecutorCompletionService<Long>(pool);
            long sumPps = 0L;
            //每个风场，每个7s发送一次数据
            Map<Integer, Integer> thinkTimeMap = new HashMap<Integer, Integer>();
            for (int cFarm = 1; cFarm <= farm; cFarm++) {
                int thinkTime = RANDOM.nextInt(sleepTime);
                thinkTimeMap.put(cFarm, thinkTime);
            }
            for (int batch = 1; batch <= batchMax; batch++) {
                long startTime = System.currentTimeMillis();
                for (int cFarm = 1; cFarm <= farm; cFarm++) {
                    String path = basePath + "/device/" + row + "/" + batch + "/" + cFarm;
                    executeAppend(adapter, cs, path, thinkTimeMap.get(cFarm));
                }
                long pps = calcThroughtPut(row, farm, cs);
                sumPps += pps;
                System.out.println("append 2." + row + "." + batch + " finished " + pps);
                long endTime = System.currentTimeMillis();
                long costTime = endTime - startTime;
                // 每七秒执行一次
                sleep(sleepTime - costTime > 0 ? sleepTime - costTime : 1);
            }
            appendResultBuffer.append("device");
            appendResultBuffer.append("\t");
            appendResultBuffer.append(row);
            appendResultBuffer.append("\t");
            appendResultBuffer.append(sumPps / batchMax);
            appendResultBuffer.append(LINE_SEPARATOR);
            pool.shutdown();
        }
        System.out.println(">>>>>>>>>>append-2 end " + System.currentTimeMillis() + ">>>>>>>>>>");
        return appendResultBuffer.toString();
    }

    /**
     * 计算append吞吐量
     * 计算吞吐量 throughtput= sum(points)/avg(timeout) ||sum(rows)/avg(timeout)
     *
     * @param row  每个farm的设备数
     * @param farm 风场数
     * @param cs   任务
     */
    private static long calcThroughtPut(int row, int farm, CompletionService<Long> cs) {
        long points = farm * row * MAX_SENSOR;//50个设备，50个传感器
        double avgTime = calAvgTimeout(farm, cs);
        long pps = 0L;
        if (avgTime == 0) {
            pps = 0L;
        } else {
            pps = (long) (points / avgTime);
        }
        return pps;
    }

    /**
     * 调用适配器，执行写入操作
     *
     * @param adapter 适配器
     * @param cs      执行线程
     * @param path    数据来源路径
     */
    private static void executeAppend(BaseAdapter adapter, CompletionService<Long> cs, String path) {
        cs.submit(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                String data = FileUtils.read(path);
                return adapter.insertData(data);
            }
        });
    }

    private static void executeAppend(BaseAdapter adapter, CompletionService<Long> cs, String path, final int sleepTime) {
        cs.submit(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                String data = FileUtils.read(path);
                Thread.currentThread().sleep(sleepTime);
                return adapter.insertData(data);
            }
        });
    }

    /**
     * 计算响应时间
     *
     * @param farm
     * @param cs
     * @return avg timeout 单位为s
     */
    private static double calAvgTimeout(int farm, CompletionService<Long> cs) {
        double sumTimeout = 0;
        int successTime = 0;
        try {
            for (int index = 1; index <= farm; index++) {
                Long timeout = cs.take().get();
                if (timeout > 0) {
                    sumTimeout += timeout;
                    successTime++;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (successTime == 0) {
            return 0;
        } else {
            return sumTimeout / successTime / 1000.0;
        }

    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////read 测试
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    private static String readPerform(BaseAdapter adapter) {
        StringBuffer resultBuffer = new StringBuffer();
        resultBuffer.append("##query  result");
        resultBuffer.append(LINE_SEPARATOR);
        int batch = 10;


        // 五类查询，每类10个批次
        long time = 1514736000000l;//2018-01-01 00:00:00
       //long time = 1514822400000l;//test
        //首先查询一次刷新数据
        adapter.query1(time, time + 100 * 3600 * 24);
        sleep(SLEEP_TIME);
        System.out.println(">>>>>>>>>>query-1 end " + System.currentTimeMillis() + ">>>>>>>>>>");
        // 每一批次比前一批次时间维度平移1hour
        long slipUnit = 3600 * 1000;
        long sumTimeout1 = 0;
        for (int cBatch = 1; cBatch <= batch; cBatch++) {
            long start = time + (cBatch - 1) * slipUnit;//1 hour
            long incre = 3600 * 1000;
            long end = time + incre + (cBatch - 1) * slipUnit;
            long timeout = adapter.query1(start, end);
            sumTimeout1 += timeout;
            System.out.println("query 1." + cBatch + " finished " + timeout);
            sleep(SLEEP_TIME);
        }
        resultBuffer.append("query1");
        resultBuffer.append("\t");
        resultBuffer.append(sumTimeout1 / batch);
        resultBuffer.append(LINE_SEPARATOR);
        System.out.println(">>>>>>>>>>query-2 end " + System.currentTimeMillis() + ">>>>>>>>>>");

        long sumTimeout2 = 0;
        for (int cBatch = 1; cBatch <= batch; cBatch++) {
            long start = time + (cBatch - 1) * slipUnit;//1 day
            long incre = 3600 * 1000 * 24;
            long end = time + incre + (cBatch - 1) * slipUnit;
            double value = 3.0;//阈值 TODO
            long timeout = adapter.query2(start, end, value);
            sumTimeout2 += timeout;
            System.out.println("query 2." + cBatch + " finished " + timeout);
            sleep(SLEEP_TIME);
        }
        resultBuffer.append("query2");
        resultBuffer.append("\t");
        resultBuffer.append(sumTimeout2 / batch);
        resultBuffer.append(LINE_SEPARATOR);
        long sumTimeout3 = 0;
        System.out.println(">>>>>>>>>>query-3 end " + System.currentTimeMillis() + ">>>>>>>>>>");

        for (int cBatch = 1; cBatch <= batch; cBatch++) {
            long start = time + (cBatch - 1) * slipUnit;// 1 day
            long incre = 3600 * 1000 * 24;
            long end = time + incre + (cBatch - 1) * slipUnit;
            long timeout = adapter.query3(start, end);
            sumTimeout3 += timeout;
            System.out.println("query 3." + cBatch + " finished " + timeout);
            sleep(SLEEP_TIME);
        }
        resultBuffer.append("query3");
        resultBuffer.append("\t");
        resultBuffer.append(sumTimeout3 / batch);
        resultBuffer.append(LINE_SEPARATOR);
        long sumTimeout4 = 0;
        System.out.println(">>>>>>>>>>query-4 end " + System.currentTimeMillis() + ">>>>>>>>>>");

        for (int cBatch = 1; cBatch <= batch; cBatch++) {
            long start = time + (cBatch - 1) * slipUnit;// 15 min
            long incre = 1000 * 15 * 50;
            long end = time + incre + (cBatch - 1) * slipUnit;
            long timeout = adapter.query4(start, end);
            sumTimeout4 += timeout;
            System.out.println("query 4." + cBatch + " finished " + timeout);
            sleep(SLEEP_TIME);
        }
        resultBuffer.append("query4");
        resultBuffer.append("\t");
        resultBuffer.append(sumTimeout4 / batch);
        resultBuffer.append(LINE_SEPARATOR);
        System.out.println(">>>>>>>>>>query-5 end " + System.currentTimeMillis() + ">>>>>>>>>>");

        long sumTimeout5 = 0;
        for (int cBatch = 1; cBatch <= batch; cBatch++) {
            long start = time + (cBatch - 1) * slipUnit;//15 min
            long incre = 1000 * 15 * 50;
            long end = time + incre + (cBatch - 1) * slipUnit;
            long timeout = adapter.query5(start, end);
            sumTimeout5 += timeout;
            System.out.println("query 5." + cBatch + " finished " + timeout);
            sleep(SLEEP_TIME);
        }
        resultBuffer.append("query5");
        resultBuffer.append("\t");
        resultBuffer.append(sumTimeout5 / batch);
        resultBuffer.append(LINE_SEPARATOR);
        return resultBuffer.toString();
    }

    private static void sleep(long sleepTime) {
        try {
            Thread.currentThread().sleep(sleepTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////输出结果////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////
    private static void writeResult(String resultPath, String data) {
        FileUtils.writeLine(resultPath, data);
    }
}

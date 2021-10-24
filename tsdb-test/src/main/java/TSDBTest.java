import cn.edu.ruc.start.TSBM;

import java.io.FileInputStream;
import java.util.Properties;

public class TSDBTest {
    private static String dataPath = "";
    public static Properties properties = null;

    //"$DB_CODE" "${TEST_METHOD}" "${DATA_PATH}"
    public static void main(String[] args) throws Exception {
//        properties = new Properties();
//        try {
//            FileInputStream in = new FileInputStream("dbInfoConfig.properties");
//            properties.load(in);
//        } catch (Exception e) {
//            System.out.println(e.toString());
//        }

//        System.out.println(args[0]);
//        System.out.println(args[1]);
//        System.out.println(args[2]);
//        dataPath = args[2];
//        if ("1".equals(args[0])) {
//            if ("1".equals(args[1])) {
//                testInfluxdb(true);
//            }
//            if ("2".equals(args[1])) {
//                testInfluxdb(false);
//            }
//            if ("0".equals(args[1])) {
//                TSBM.generateData(dataPath);
//            }
//        }
//        if ("2".equals(args[0])) {
//            if ("0".equals(args[1])) {
//                TSBM.generateData(dataPath);
//            }
//            if ("1".equals(args[1])) {
//                testTimescaledb(true);
//            }
//            if ("2".equals(args[1])) {
//                testTimescaledb(false);
//            }
//        }
//        if ("3".equals(args[0])) {
//            if ("0".equals(args[1])) {
//                TSBM.generateData(dataPath);
//            }
//            if ("1".equals(args[1])) {
//                testIotdb(true);
//            }
//            if ("2".equals(args[1])) {
//                testIotdb(false);
//            }
//        }
//        if ("4".equals(args[0])) {
//            if ("0".equals(args[1])) {
//                TSBM.generateData(dataPath);
//            }
//            if ("1".equals(args[1])) {
//                testOpentsdb(true);
//            }
//            if ("2".equals(args[1])) {
//                testOpentsdb(false);
//            }
//        }
//        if ("5".equals(args[0])) {
//            if ("0".equals(args[1])) {
//                TSBM.generateData(dataPath);
//            }
//            if ("1".equals(args[1])) {
//                testDruid(true);
//            }
//            if ("2".equals(args[1])) {
//                testDruid(false);
//            }
//        }
//        if ("6".equals(args[0])) {
//            if ("0".equals(args[1])) {
//                TSBM.generateData(dataPath);
//            }
//            if ("1".equals(args[1])) {
//                testGaussDBForInfluxdb(true);
//            }
//            if ("2".equals(args[1])) {
//                testGaussDBForInfluxdb(false);
//            }
//        }
//
//        if ("7".equals(args[0])) {
//            if ("0".equals(args[1])) {
//                TSBM.generateData(dataPath);
//            }
//            if ("1".equals(args[1])) {
//                testTDEngine(true);
//            }
//            if ("2".equals(args[1])) {
//                testTDEngine(false);
//            }
//        }
//        if ("8".equals(args[0])) {
//            if ("0".equals(args[1])) {
//                TSBM.generateData(dataPath);
//            }
//            if ("1".equals(args[1])) {
//                testAliHiTSDB(true);
//            }
//            if ("2".equals(args[1])) {
//                testAliHiTSDB(false);
//            }
//        }
//        if ("9".equals(args[0])) {
//            if ("0".equals(args[1])) {
//                TSBM.generateData(dataPath);
//            }
//            if ("1".equals(args[1])) {
//                testAliLindorm(true);
//            }
//            if ("2".equals(args[1])) {
//                testAliLindorm(false);
//            }
//        }
//        if ("10".equals(args[0])) {
//            if ("0".equals(args[1])) {
//                TSBM.generateData(dataPath);
//            }
//            if ("1".equals(args[1])) {
//                testAliInfluxDB(true);
//            }
//            if ("2".equals(args[1])) {
//                testAliInfluxDB(false);
//            }
//        }
//        if ("11".equals(args[0])) {
//            if ("0".equals(args[1])) {
//                TSBM.generateData(dataPath);
//            }
//            if ("1".equals(args[1])) {
//                testKdbPlus(true);
//            }
//            if ("2".equals(args[1])) {
//                testKdbPlus(false);
//            }
//        }
        TSBM.startPerformTest("/Users/hsw/Desktop/JetBrains/IDEAproject/TS-Benchmark/test",
                "cn.edu.ruc.KdbPlusAdapter","127.0.0.1","5001","",""
        ,false,false);
    }

    private static void testKdbPlus(boolean loadParam) {

        String className = "cn.edu.ruc.KdbPlusAdapter";
        String ip = properties.getProperty("KdbPlus_ip");
        String port = properties.getProperty("KdbPlus_port");
        String userName = properties.getProperty("KdbPlus_username");
        String passwd = properties.getProperty("KdbPlus_password");
        TSBM.startPerformTest(dataPath, className, ip, port, userName, passwd, false, loadParam);
    }

    public static void testIotdb(boolean loadParam) {
//        String dataPath = dataPath;
        String className = "cn.edu.ruc.IotdbAdapterNativeApi";
        String ip = properties.getProperty("IoTDB_ip");
        String port = properties.getProperty("IoTDB_port");
        String userName = properties.getProperty("IoTDB_username");
        String passwd = properties.getProperty("IoTDB_password");
        TSBM.startPerformTest(dataPath, className, ip, port, userName, passwd, false, loadParam);
    }

    public static void testInfluxdb(boolean loadParam) {
//        String dataPath = "/Users/fasape/project/tsdb-test/";
        String className = "cn.edu.ruc.InfluxdbAdapter";
        String ip = properties.getProperty("Influx_ip");
        String port = properties.getProperty("Influx_port");
        String userName = properties.getProperty("Influx_username");
        String passwd = properties.getProperty("Influx_password");
        TSBM.startPerformTest(dataPath, className, ip, port, userName, passwd, false, loadParam);
    }

    public static void testGaussDBForInfluxdb(boolean loadParam) {
        //String dataPath = "/Users/fasape/project/tsdb-test/";
        String istestdb = properties.getProperty("GaussdbForInflux_isTest");
        if (!istestdb.equalsIgnoreCase("true")) {
            return;
        }
        String className = "cn.edu.ruc.gaussDBForInfluxAdapter2";
        String ip = properties.getProperty("GaussdbForInflux_ip");
        String port = properties.getProperty("GaussdbForInflux_port");
        String userName = properties.getProperty("GaussdbForInflux_username");
        String passwd = properties.getProperty("GaussdbForInflux_password");
        TSBM.startPerformTest(dataPath, className, ip, port, userName, passwd, false, loadParam);
    }

    public static void testTimescaledb(boolean loadParam) {
//        String dataPath = "/Users/fasape/project/tsdb-test/";
        String className = "cn.edu.ruc.TimescaledbAdapter";
        String ip = "127.0.0.1";
        String port = "5432";
        String userName = "postgres";
        String passwd = "postgres";
        TSBM.startPerformTest(dataPath, className, ip, port, userName, passwd, false, loadParam);
    }

    public static void testOpentsdb(boolean loadParam) {
        String className = "cn.edu.ruc.OpentsdbAdapter";
        String ip = "127.0.0.1";
        String port = "8242";
        String userName = "root"; //not required
        String passwd = "root"; //not required
        TSBM.startPerformTest(dataPath, className, ip, port, userName, passwd, false, loadParam);
    }

    public static void testDruid(boolean loadParam) {
        String className = "cn.edu.ruc.DruidAdapter";
        String ip = properties.getProperty("Druid_ip");
        String port = properties.getProperty("Druid_port");
        ;
        String userName = properties.getProperty("Druid_username"); //not required
        String passwd = properties.getProperty("Druid_password"); //not required
        TSBM.startPerformTest(dataPath, className, ip, port, userName, passwd, false, loadParam);
    }

    public static void testTDEngine(boolean loadParam) {
        String className = "cn.edu.ruc.TdengineAdapter2";
        String ip = properties.getProperty("TDEngine_ip");
        String port = properties.getProperty("TDEngine_port");
        String userName = properties.getProperty("TDEngine_username"); //not required
        String passwd = properties.getProperty("TDEngine_password"); //not required
        TSBM.startPerformTest(dataPath, className, ip, port, userName, passwd, false, loadParam);
    }

    public static void testAliHiTSDB(boolean loadParam) {
        String className = "cn.edu.ruc.AliHiTSDBAdapter";
        String ip = properties.getProperty("AliHiTSDB_ip");
        String port = properties.getProperty("AliHiTSDB_port");
        String userName = properties.getProperty("AliHiTSDB_username");
        String passwd = properties.getProperty("AliHiTSDB_password");
        TSBM.startPerformTest(dataPath, className, ip, port, userName, passwd, false, loadParam);
    }

    public static void testAliLindorm(boolean loadParam) {
        String className = "cn.edu.ruc.AliLindormAdapter";
        String ip = properties.getProperty("AliLindorm_ip");
        String port = properties.getProperty("AliLindorm_port");
        String userName = properties.getProperty("AliLindorm_username");
        String passwd = properties.getProperty("AliLindorm_password");
        TSBM.startPerformTest(dataPath, className, ip, port, userName, passwd, false, loadParam);
    }

    public static void testAliInfluxDB(boolean loadParam) {
        String className = "cn.edu.ruc.AliInfluxAdapter";
        String ip = properties.getProperty("AliInflux_ip");
        String port = properties.getProperty("AliInflux_port");
        String userName = properties.getProperty("AliInflux_username"); //not required
        String passwd = properties.getProperty("AliInflux_password"); //not required
        TSBM.startPerformTest(dataPath, className, ip, port, userName, passwd, false, loadParam);
    }
}

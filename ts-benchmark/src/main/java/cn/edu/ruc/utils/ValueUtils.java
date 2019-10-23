package cn.edu.ruc.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ValueUtils {

    private static long IMPORT_START_TIME=1514736000000L;
    private static final Random RANDOM = new Random();
    public static double getValueByField(int fNum,int sNum,long time){
        sNum=sNum%50;// 增加鲁棒性
        if(sNum==0){
            sNum=50;
        }
        time=(time-IMPORT_START_TIME)/7000;
        double cFactor = (RANDOM.nextDouble()/100 +0.99)*(1+(fNum-1)/100);
        return getValueBySensor(sNum,time)*cFactor;
    }
    private static double getValueBySensor(int sNum,long time){
        if(sNum<=26){
            return SENSOR_DATA.get((int)time)[sNum-1];
        }else{
            //线性9；傅立叶6；分段9；
            double value=0;
            switch (sNum){
                //线性9
                case 27:
                    value= getMonoValue(17376.5,17366.5,5000,time);
                    break;
                case 28:
                    value= getMonoValue(33919.6,33909.6,5000,time);
                    break;
                case 29:
                    value= getMonoValue(35061,35051,5000,time);
                    break;
                case 30:
                    value= getMonoValue(35061,35051,5000,time);
                    break;
                case 31:
                    value= getMonoValue(6001,6000,5600,time);
                    break;
                case 32:
                    value= getMonoValue(6001,6000,6000,time);
                    break;
                case 33:
                    value= getMonoValue(34704.8,33909.7,10000,time);
                    break;
                case 34:
                    value= getMonoValue(33695.1,33501,10000,time);
                    break;
                case 35:
                    value= getMonoValue(34041.5,33847.4,10000,time);
                    break;
                // sin
                case 36:
                    value= getSineValue(6.59887,3.54113,1754,time);
                    break;

                case 37:
                    value= getMonoValue(6.65208,3.59392,1757,time);
                    break;
                case 38:
                    value= getMonoValue(6.53903,3.26297,1802,time);
                    break;
                case 39:
                    value= getMonoValue(6.59887,3.54113,1754,time);
                    break;
                case 40:
                    value= getMonoValue(6.65208,3.59392,1757,time);
                    break;
                case 41:
                    value= getMonoValue(6.34178,3.46022,1799,time);
                    break;
                // 分段
                case 42:
                    value= getMonoValue(5,2,2600,time);
                    break;

                case 43:
                    value= getMonoValue(2,0,48000,time);
                    break;
                case 44:
                    value= getMonoValue(6,0,4500,time);
                    break;
                case 45:
                    value= getMonoValue(1483.42,1473.54,60000,time);
                    break;
                case 46:
                    value= getMonoValue(36.74,25.93,3000,time);
                    break;
                case 47:
                    value= getMonoValue(0.99,0,4500,time);
                    break;
                case 48:
                    value= getMonoValue(787.74,781.54,16000,time);
                    break;
                case 49:
                    value= getMonoValue(787.74,781.54,16000,time);
                    break;
                case 50:
                    value= getMonoValue(787.74,781.54,16000,time);
                    break;
                default:
                    break;


            }

            return value;
        }
    }
    private static double getMonoValue(double max,double min,double cycle,long currentTime){
        double k=(max-min)/(cycle);
        return k*(currentTime)+min;
    }
    private static double getSineValue(double max,double min,double cycle,long currentTime){
        double w=2*Math.PI/cycle;
        double a=(max-min)/2;
        double b=(max-min)/2;
        return Math.sin(w*(currentTime%cycle))*a+b+min;
    }
    private static double getSquareValue(double max,double min,double cycle,long currentTime){
        double t = cycle/2;
        if((currentTime%(cycle))<t){
            return max;
        }else{
            return min;
        }
    }
    private static List<Double[]> SENSOR_DATA=new ArrayList<Double[]>();
    static{
        try {
            InputStream is = ValueUtils.class.getClassLoader().getResourceAsStream("14_data.csv");
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            String s="";
            while((s= reader.readLine())!=null){
                if(StringUtils.isNotBlank(s)){
                    String[] values = s.split(",");
                    Double[] valueDoubles=new Double[values.length-1];
                    for(int i=1;i<values.length;i++){
                        valueDoubles[i-1]=Double.parseDouble(values[i]);
                    }
                    SENSOR_DATA.add(valueDoubles);
                }

            }
            is.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}

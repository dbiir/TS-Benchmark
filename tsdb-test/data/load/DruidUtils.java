import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
public class DruidUtils {

	public void transformFile(String fName) throws IOException {
		//String fname="/home/tsbm/tsbm_app/benchmark/mini-tsdb-test/tsdb-test/data/load/load.data";     
		fName +="/load.data";
		FileReader fr=new FileReader(fName);
		BufferedReader br=new BufferedReader(fr);
		FileWriter fw=new FileWriter("load_druid_json.txt");
		BufferedWriter bw=new BufferedWriter(fw);
		String line=";";
		String data[]=new String[5];
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		while((line=br.readLine())!=null)
		{
		 if(line.trim().isEmpty()) {
			 continue;
		 }
			data=line.split(",");
		   for(int i=3;i<53;i++) {
			   Map<String,Object> json=new HashMap<String,Object>();
			   bw.write("{\"timestamp\":\"");
			   bw.write(format.format(Long.valueOf(data[0])));
			   bw.write("\",\"f\":\"");
			   bw.write(data[1]);
			   bw.write("\",\"d\":\"");
			   bw.write(data[2]);
			   bw.write("\",\"s\":\"");
			   bw.write("s"+(i-2)+"");
			   bw.write("\",\"value\":");
			   bw.write(data[i]);
			   bw.write("}\n");
		
		    }
		 }
		bw.close();
		fw.close();
		br.close();
		fr.close();
	}
	public static void main(String[] args) throws IOException {
		String fileName=args[0];		
		DruidUtils du=new DruidUtils();
		du.transformFile(fileName);
	}
}

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class OpentsdbUtils {
    public void transformFile(String fName) throws IOException {
        fName += "/load.data";
        FileReader fr = new FileReader(fName);
        BufferedReader br = new BufferedReader(fr);
        FileWriter fw = new FileWriter("load_opentsdb_format.txt");
        BufferedWriter bw = new BufferedWriter(fw);
        String line = ";";
        String data[] = new String[153];
        int flag = 0;
        while ((line = br.readLine()) != null && flag < 12857142) {
            if (line.trim().isEmpty()) {
                continue;
            }
            flag++;
            data = line.split(",");
            for (int i = 3; i < 53; i++) {
                bw.write("w.d" + " ");
                bw.write(data[0]);
                bw.write(" ");
                bw.write(data[i]);
                bw.write(" ");
                bw.write("f=" + data[1]);
                bw.write(" ");
                bw.write("d=" + data[2]);
                bw.write(" ");
                bw.write("s=s" + (i - 2) + "\n");

            }
        }
        bw.close();
        fw.close();
        br.close();
        fr.close();
    }

    public static void main(String[] args) throws IOException {
        String fName = args[0];
        OpentsdbUtils opu = new OpentsdbUtils();
        opu.transformFile(fName);
    }
}


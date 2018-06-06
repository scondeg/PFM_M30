package main.java.FileUtilities;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URL;

public class HistoricosEntryPoint {

    private static String urlEstados = "http://www.mc30.es/images/xml/historicousuarios.xml";
    private static String pathLocal = "/home/pfm/dataPFM/estadoTrafico/historico/historicousuarios.xml";
    private static String pathDst = "/HistoricoEstadoTotales/RAW/";

    public static void downloadAndSaveToHdfsFile(){
        downloadUsingStream();
        saveToHDFS(pathLocal,pathDst);
    }

    private static void downloadUsingStream(){
        try {
            URL url = new URL(urlEstados);
            BufferedInputStream bis = new BufferedInputStream(url.openStream());
            FileOutputStream fis = new FileOutputStream(pathLocal);
            byte[] buffer = new byte[1024];
            int count=0;
            while((count = bis.read(buffer,0,1024)) != -1)
            {
                fis.write(buffer, 0 , count);
            }
            fis.close();
            bis.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void saveToHDFS(String pathSrc, String pathDst) {
        Configuration conf = new Configuration();
        conf.addResource(new Path("/opt/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/opt/hadoop/etc/hadoop/hdfs-site.xml"));

        try {
            System.out.println("Guardando historico en HDFS");
            FileSystem fs = FileSystem.get(conf);
            fs.copyFromLocalFile(new Path(pathSrc), new Path(pathDst));
            System.out.println("Guardado historico en HDFS");
        } catch (IOException e) {
            System.out.println("Error guardadno en HDFS\n" + e.getMessage());
        }
    }
}

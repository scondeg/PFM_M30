package  main.java.FileUtilities;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;

public class HistoricoIncidenciasAnualHelper {

    static final String pathDst = "/IncidenciasHistoricoAnual/RAW/";
    static final String pathSrc = "/home/pfm/dataPFM/incidencias/historico/historicoAnual/";

    public static boolean storeRawToHdfs() {

        final File folder = new File(pathSrc);
        listFilesForFolder(folder);
        return true;
    }


    private static void listFilesForFolder(final File folder) {
        File[] listOfFiles = folder.listFiles();
        String yearPartition = null;
        String pathAbsouluteSrc;
        String pathFolderDst;
        String pathFileDst;

        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
                yearPartition = listOfFiles[i].getName().substring(19, 23);
                pathAbsouluteSrc = pathSrc + listOfFiles[i].getName();
                pathFolderDst = pathDst + yearPartition + "/";
                pathFileDst = listOfFiles[i].getName();
                saveToHDFS(pathAbsouluteSrc, pathFolderDst, pathFileDst);
                listOfFiles[i].delete();
            }
        }
    }

    private static void saveToHDFS(String patAbsolutehSrc, String pathDst, String fileDst) {
        Configuration conf = new Configuration();
        conf.addResource(new org.apache.hadoop.fs.Path("/opt/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new org.apache.hadoop.fs.Path("/opt/hadoop/etc/hadoop/hdfs-site.xml"));

        org.apache.hadoop.fs.Path src = new org.apache.hadoop.fs.Path(patAbsolutehSrc);
        org.apache.hadoop.fs.Path dst = new org.apache.hadoop.fs.Path(pathDst);
        org.apache.hadoop.fs.Path dstAbsolute = new org.apache.hadoop.fs.Path(pathDst + fileDst);


        try {
            System.out.println("Guardando historico en HDFS");
            FileSystem fs = FileSystem.get(conf);
            if(!fs.exists(dst)){
                fs.mkdirs(dst);
            }
            fs.copyFromLocalFile(src, dstAbsolute);
            System.out.println("Guardado historico en HDFS");
        } catch (IOException e) {
            System.out.println("Error guardadno en HDFS\n" + e.getMessage());
        }
    }
}
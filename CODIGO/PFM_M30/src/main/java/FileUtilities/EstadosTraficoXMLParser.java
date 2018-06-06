package main.java.FileUtilities;

import org.apache.commons.io.IOUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

public class EstadosTraficoXMLParser {

    private static String urlIncidencias = "http://www.mc30.es/components/com_hotspots/datos/estado_trafico.xml";
    private static String pathLocal = "/home/pfm/dataPFM/estadoTrafico/tr/estado_traficoTR.xml";

    private static void downloadUsingStream(){
        URL url = null;
        try {
            url = new URL(urlIncidencias);
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
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static HashMap<String, String> getEstadosTrafico(){
        String estadosTraficoPlainJSON = null;
        String[] estadosTraficoStringArray = null;
        HashMap estadosTraficoHash = null;
        downloadUsingStream();
        try (InputStream inputStream = new FileInputStream(new File(
                pathLocal))){
//                "/home/pfm/IdeaProjects/PFM_M30/files/EstadoTrafico.xml"))){
//                "/home/pfm/IdeaProjects/PFM_M30/files/EstadoTraficoTest.xml"))){
            String xml = IOUtils.toString(inputStream);
            JSONObject jObject = XML.toJSONObject(xml);
            //Se comprueba que el XML obtenido tenga informacion, en caso contrario no se crea JSON y se devuelve null
            if(!jObject.get("Estados_trafico").equals("")) {
                estadosTraficoStringArray = xml.replace("<?xml version=\"1.0\" encoding=\"utf-8\"?>","").
                        replace("\n","").
                        replace(" ","").
                        replace("<Estados_trafico>","").
                        replace("<Estado_trafico>","").
                        replace("<Posicion>","").
                        replace("<Latitud>","<\"Latitud\":\"").
                        replace("</Latitud>","").
                        replace("<Longitud>","<\"Longitud\":\"").
                        replace("</Longitud>","").
                        replace("</Posicion>","").
                        replace("<Color>","<\"Color\":\"").
                        replace("</Color>","").
                        replace("</Estado_trafico>","").
                        replace("</Estados_trafico>","").
                        split("<");

                estadosTraficoHash =  new HashMap();
                int hashKeyCounter = 1;
                int lastEstadosGroup = 1;
                Date date = new Date();
                String fecha = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(date);

                for (int i = 1; i < estadosTraficoStringArray.length; i++){
                    if (estadosTraficoStringArray[i].contains("Latitud")){
                        estadosTraficoHash.put(hashKeyCounter,"{" + estadosTraficoStringArray[i] + "\"");
                    } else if (estadosTraficoStringArray[i].contains("Longitud")){
                        estadosTraficoHash.put(hashKeyCounter, estadosTraficoHash.get(hashKeyCounter) + "," + estadosTraficoStringArray[i] + "\"");
                        hashKeyCounter++;
                    } else if (estadosTraficoStringArray[i].contains("Color")){
                        for (int c = lastEstadosGroup; c<hashKeyCounter; c++){
                            estadosTraficoHash.put(c, estadosTraficoHash.get(c) + "," + estadosTraficoStringArray[i] + "\"" + ",\"Fecha\":\"" + fecha + "\"}");
                        }
                        lastEstadosGroup = hashKeyCounter;
                    }
                }
                System.out.println(estadosTraficoPlainJSON);
            } else {
                System.out.println("XML EstadosTrafico vacío");
            }
            return estadosTraficoHash;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return estadosTraficoHash;
    }
}

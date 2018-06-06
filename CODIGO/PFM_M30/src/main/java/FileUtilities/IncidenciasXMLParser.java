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

public class IncidenciasXMLParser {

    private static String urlIncidencias = "http://www.mc30.es/components/com_hotspots/datos/incidencias.xml";
    private static String pathLocal = "/home/pfm/dataPFM/incidencias/tr/incidenciasTR.xml";

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

    public static HashMap<String, String> getIncidencias(){
        String[] incidenciasStringArray = null;
        HashMap incidenciasHash = null;
        downloadUsingStream();
        try (InputStream inputStream = new FileInputStream(new File(
                pathLocal))){
//                "/home/pfm/IdeaProjects/PFM_M30/files/IncidenciasTrafico.xml"))){
//            "/home/pfm/IdeaProjects/PFM_M30/files/IncidenciasTraficoCerrada.xml"))){
//                "/home/pfm/IdeaProjects/PFM_M30/files/IncidenciasEmpty.xml"))){
            String xml = IOUtils.toString(inputStream);
            JSONObject jObject = XML.toJSONObject(xml);
            //Se comprueba que el XML obtenido tenga informacion, en caso contrario no se crea JSON y se devuelve null
            if(!jObject.get("Incidencias").equals("")) {
                incidenciasStringArray = xml.replace("<?xml version=\"1.0\" encoding=\"utf-8\"?>","").
                        replace("\n","").
                        replace(" ","").
                        replace("<Incidencias>","").
                        replace("<Incidencia>","").
                        replace("<Posicion>","").
                        replace("<Latitud>","<\"Latitud\":\"").
                        replace("</Latitud>","").
                        replace("<Longitud>","<\"Longitud\":\"").
                        replace("</Longitud>","").
                        replace("</Posicion>","").
                        replace("<Identificador>","<\"Identificador\":\"").
                        replace("</Identificador>","").
                        replace("<Codigo>","<\"Codigo\":\"").
                        replace("</Codigo>","").
                        replace("<Texto>","<\"Texto\":\"").
                        replace("</Texto>","").
                        replace("<FechaCierre>","<\"FechaCierre\":\"").
                        replace("</FechaCierre>","").
                        replace("</Incidencia>","").
                        replace("</Incidencias>","").
                        split("<");

                incidenciasHash =  new HashMap();
                int hashKeyCounter = 1;
                Date date = new Date();
                String fecha = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(date);

                for (int i = 1; i < incidenciasStringArray.length; i++){
                    if (incidenciasStringArray[i].contains("Latitud")){
                        incidenciasHash.put(hashKeyCounter,"{" + incidenciasStringArray[i] + "\"");
                    } else if (incidenciasStringArray[i].contains("Longitud")){
                        incidenciasHash.put(hashKeyCounter, incidenciasHash.get(hashKeyCounter) + "," + incidenciasStringArray[i] + "\"");
                    } else if (incidenciasStringArray[i].contains("Identificador")){
                        incidenciasHash.put(hashKeyCounter, incidenciasHash.get(hashKeyCounter) + "," + incidenciasStringArray[i] + "\"");
                    } else if (incidenciasStringArray[i].contains("Codigo")){
                        incidenciasHash.put(hashKeyCounter, incidenciasHash.get(hashKeyCounter) + "," + incidenciasStringArray[i] + "\"");
                    } else if (incidenciasStringArray[i].contains("Texto")){
                        incidenciasHash.put(hashKeyCounter, incidenciasHash.get(hashKeyCounter) + "," + incidenciasStringArray[i] + "\"");
                    } else if (incidenciasStringArray[i].contains("FechaCierre")){
                        incidenciasHash.put(hashKeyCounter, incidenciasHash.get(hashKeyCounter) + "," + incidenciasStringArray[i] + "\"" + ",\"Fecha\":\"" + fecha + "\"}");
                        hashKeyCounter++;
                    }
                }
            } else {
                System.out.println("XML Incidencias vacío");
            }
            return incidenciasHash;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return incidenciasHash;
    }
}

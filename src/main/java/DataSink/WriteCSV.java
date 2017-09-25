package DataSink;

import DataSource.SparkAPI;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.regex.Matcher;

public class WriteCSV {
    public static void main(String[] args) throws Exception {

        SparkAPI instance = new SparkAPI();
        instance.setUp();
        instance.authenticate();

        DateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        int gap = 12;
        String freq = "day";
        ZonedDateTime end = ZonedDateTime.now();
        ZonedDateTime start = end.minusMonths(gap);
        List<Long> siteIds = instance.listSitesIds();



//        Long siteId = 19640L;157185L <<<<< even better one;
//        Long siteId = 27827L;
//        List<Long> resourcesIds = Arrays.asList(90771L,90777L,90766L,90784L,205593L,157421L,205594L,202126L,157425L,205595L,205592L,157476L,157868L,205589L);



        for (Long siteId : siteIds) {
            siteId = 27827L;
            List<Long> resourcesIds = instance.listResourcesIds(siteId);

            String property = "Temperature";
            for (Long resourceId : resourcesIds) {
                try{
                    if (instance.getResourceIdDetails(resourceId,"property").matches(property)){
                        System.out.println("\""+siteId+"_"+resourceId+".csv\",");
                        String file_name = "./src_python/" + siteId + "_" + resourceId + ".csv";
//                        resouceWriteCSV(instance, format, freq, end, start, resourceId, file_name);
                    }
                }
                catch (Exception e){
                    System.out.println(e+" "+ resourceId);
                    continue;
                }
            }
            List<Long> subsitesIds = instance.listSubsites(siteId);
            for (Long subsiteId : subsitesIds) {
                List<Long> sub_resourcesIds = instance.listResourcesIds(subsiteId);
                for (Long resourceId : sub_resourcesIds) {
                    try{
                        if (instance.getResourceIdDetails(resourceId,"property").matches(property)){
                            System.out.println("\""+siteId+"_"+subsiteId+"_"+resourceId+".csv\",");
                            String file_name = "./src_python/" + siteId + "_" + subsiteId + "_" + resourceId + ".csv";
//                            resouceWriteCSV(instance, format, freq, end, start, resourceId, file_name);
                        }
                    }
                    catch (Exception e){
                        System.out.println(e+" "+ resourceId);
                        continue;
                    }
                }
            }
            break;
        }
    }




    public static void resouceWriteCSV(SparkAPI instance, DateFormat format, String freq, ZonedDateTime end, ZonedDateTime start, Long resourceId, String file_name) throws IOException {
        FileWriter fw = new FileWriter(file_name, true);
        try{
            Matcher matcher = instance.getResourceDataByDayRange(resourceId, start, end, freq);
            while (matcher.find()) {
                String group = matcher.group();
                Date time = new Date(Long.parseLong(group.split(",")[1].split("=")[1].split("}")[0]));
                String timestamp = format.format(time);
                float reading = Float.parseFloat(group.split(",")[0].split("=")[1]);
                fw.write(timestamp + ";" + reading + "\n");
            }
            fw.close();
        }
        catch (Exception e){
            System.out.println(e+" SOMETHING WRONG WITH "+ resourceId);
        }
    }
}

package DataSink;

import DataSource.SparkAPI;
import java.io.FileWriter;
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

        Long siteId = 19640L;
        List<Long> resourcesIds = instance.listResourcesIds(siteId);
        String property = "Calculated Power Consumption";
        for (Long resourceId : resourcesIds) {
            try{
                if (instance.getResourceIdDetails(resourceId,"property").matches(property)){
                    System.out.println("\""+siteId+"_"+resourceId+".csv\",");
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
                    }
                }
                catch (Exception e){
                    System.out.println(e+" "+ resourceId);
                    continue;
                }
            }
        }




//        Long siteId = 27827L;
//        List<Long> resourcesIds = Arrays.asList(90771L,90777L,90766L,90784L,205593L,157421L,205594L,202126L,157425L,205595L,205592L,157476L,157868L,205589L);

/* for collecting all the data
        DateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        int gap = 4;
        String freq = "hour";
        ZonedDateTime end = ZonedDateTime.now();
        ZonedDateTime start = end.minusWeeks(gap);

        List<Long> siteIds = instance.listSitesIds();
        for (Long siteId : siteIds) {
            List<Long> resourcesIds = instance.listResourcesIds(siteId);
            for (Long resourceId : resourcesIds) {
                String file_name = "./src_python/" + siteId + "_" + resourceId + ".csv";
                System.out.println(file_name);
                FileWriter fw = new FileWriter(file_name, true);
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

            List<Long> subsitesIds = instance.listSubsites(siteId);
            for (Long subsiteId : subsitesIds) {
                List<Long> sub_resourcesIds = instance.listResourcesIds(subsiteId);
                for (Long resourceId : sub_resourcesIds) {
                    String file_name = "./src_python/" + siteId + "_" + subsiteId + "_" + resourceId + ".csv";
                    System.out.println(file_name);
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
                        continue;
                    }

                }
            }
        }
        */
    }
}

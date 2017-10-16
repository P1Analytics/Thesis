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

    public static void WriteCSV(SparkAPI instance, DateFormat format, String freq,
                                ZonedDateTime start,ZonedDateTime end,
                                Long resourceId, String file_name) throws IOException {

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


    public static void main(String[] args) throws Exception {
        SparkAPI instance = new SparkAPI();
        instance.setUp();
        instance.authenticate();
        String folder = "./src_python/";

        DateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

        int gap = 4; //weeks
        String freq = "5min"; //5min,hour,day,
        ZonedDateTime end = ZonedDateTime.now();
        ZonedDateTime start = end.minusWeeks(gap);

//        List<Long> resourcesIds = Arrays.asList(90771L,90777L,90766L,90784L,205593L,157421L,205594L,202126L,157425L,205595L,205592L,157476L,157868L,205589L);

//        List<Long> siteIds = instance.listSitesIds();
        List<Long> siteIds = Arrays.asList(
                                           144242L, 27827L,144024L, 155076L,
                                           155849L, 155077L, 155865L, 155877L,
                                           28843L, 144243L, 28850L, 159705L, 157185L, 155851L, 19640L);
        for (Long siteId : siteIds) {
            String property = "Luminosity";

            List<Long> resourcesIds = instance.listResourcesIds(siteId);
            for (Long resourceId : resourcesIds) {
                try{
                    if (instance.getResourceIdDetails(resourceId,"property").contains(property)){
                        String id = instance.getResourceSummary(resourceId,freq);
                        System.out.println("\""+siteId+"_"+resourceId+"_"+ id+".csv\",");
                        String file_name = folder + siteId + "_" + resourceId +"_"+ id+".csv";
                        WriteCSV(instance, format, freq, start,end, resourceId, file_name);
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
                        if (instance.getResourceIdDetails(resourceId,"property").contains(property)){
                            String id = instance.getResourceSummary(resourceId,freq);
                            System.out.println("\""+siteId+"_"+subsiteId+"_"+resourceId+"_"+id+".csv\",");
                            String file_name = folder + siteId + "_" + subsiteId + "_" + resourceId +"_"+id+".csv";
                            WriteCSV(instance, format, freq, start,end,  resourceId, file_name);
                        }
                    }
                    catch (Exception e){
                        System.out.println(e+" "+ resourceId);
                        continue;
                    }
                }
            }
            System.out.println();
        }
    }
}

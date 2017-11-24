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

    private static void WriteToCSV(SparkAPI instance, DateFormat format, String freq,
                                   ZonedDateTime start, ZonedDateTime end,
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
            System.out.println(e +" SOMETHING WRONG WITH "+ resourceId);
        }
    }


    public static void main(String[] args) throws Exception {
        SparkAPI instance = new SparkAPI();
        instance.setUp();
        instance.authenticate();

//        DateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm");
        DateFormat format = new SimpleDateFormat("yyyy/MM/dd");

        String freq = "5min"; //5min,hour,day,
        ZonedDateTime start = ZonedDateTime.parse("2017-02-01T00:00:00+01:00[Europe/Rome]");
        ZonedDateTime end = start.plusMonths(1);

        String folder = "./src_python/";
        String property = "Temperature";

//        List<Long> siteIds = Arrays.asList(28843L, 144243L, 28850L, 159705L, 157185L, 155851L, 19640L);
//          [144242L, 27827L, 144024L, 155076L, 155849L, 155077L, 155865L, 155877L, 28843L, 144243L, 28850L, 159705L, 157185L, 155851L, 19640L]
//        for (Long siteId : siteIds) {

        for (Long siteId : instance.listSitesIds()) {
            List<Long> resourcesIds = instance.listResourcesIds(siteId);
            for (Long resourceId : resourcesIds) {
                try{
                    if (instance.getResourceIdDetails(resourceId,"property").contains(property)){
                        String id = instance.getResourceSummary(resourceId,freq);
                        String file_name = folder + siteId + "_"
                                + resourceId +"_"+ id+"_"
                                +instance.getResourceIdDetails(resourceId,"property")+".csv";
                        WriteToCSV(instance, format, freq, start,end, resourceId, file_name);
                        System.out.println("\""+siteId + "_" + resourceId +"_"+ id+"_"+instance.getResourceIdDetails(resourceId,"property")+".csv\",");
                    }
                }
                catch (Exception e){
                    System.out.println(e+" "+ resourceId);
                }
            }

            List<Long> subsitesIds = instance.listSubsites(siteId);
            for (Long subsiteId : subsitesIds) {
                List<Long> sub_resourcesIds = instance.listResourcesIds(subsiteId);
                for (Long resourceId : sub_resourcesIds) {
                    try{
                        if (instance.getResourceIdDetails(resourceId,"property").contains(property)){
                            String id = instance.getResourceSummary(resourceId,freq);
                            String file_name = folder + siteId + "_"
                                    + subsiteId + "_" + resourceId +"_"+id+"_"
                                    +instance.getResourceIdDetails(resourceId,"property")+".csv";
                            WriteToCSV(instance, format, freq, start,end,  resourceId, file_name);
                            System.out.println("\""+siteId+"_"+subsiteId+"_"+resourceId+"_"+id+"_"+instance.getResourceIdDetails(resourceId,"property")+".csv\",");
                        }
                    }
                    catch (Exception e){
                        System.out.println(e+" "+ resourceId);
                    }
                }
            }
//            break; // when run for one site only
        }
    }
}

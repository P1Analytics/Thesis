package DataSink;

import DataSource.SparkAPI;
import java.io.FileWriter;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

public class WriteCSV {
    public static void main (String[] args) throws Exception {

        SparkAPI instance = new SparkAPI();
        instance.setUp();
        instance.authenticate();

        ZonedDateTime end = ZonedDateTime.now();
        ZonedDateTime start = end.minusMonths(3);

        List<Long> siteIds = instance.listSitesIds();
        for (Long siteId : siteIds){
            List<Long> resourcesIds = instance.listResourcesIds(siteId);
            for (Long resourceId: resourcesIds){
                String freq = "day";

                String file_name = resourceId+"_"+freq+".csv";
                FileWriter fw = new FileWriter(file_name,true);

                Matcher matcher = instance.getResourceDataByDayRange(resourceId, start, end, freq);
                List<Float> matches = new ArrayList<Float>();
                while (matcher.find()) {
                    String group = matcher.group();
                    fw.write(group+"\n");
                    float reading = Float.parseFloat(group.split(",")[0].split("=")[1]);
                    matches.add(reading);
                    String timestamp = group.split(",")[1].split("=")[1].split("}")[0];
                }
                fw.close();
                System.out.println(matches);
            }
        }
    }
}
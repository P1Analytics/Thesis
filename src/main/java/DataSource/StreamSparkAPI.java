package DataSource;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

public class StreamSparkAPI {
    public static void main (String[] args) throws Exception {
        SparkAPI instance = new SparkAPI();
        instance.setUp();
        instance.authenticate();

        ZonedDateTime end = ZonedDateTime.now();
        ZonedDateTime start = end.minusWeeks(1);
//        ZonedDateTime start = ZonedDateTime.now().minusHours(1);
//        ZonedDateTime start = ZonedDateTime.now().minusDays(1);
//        ZonedDateTime start = ZonedDateTime.now().minusMonths(3);
//        ZonedDateTime start = ZonedDateTime.now().minusYears(5);

//        instance.getResourceDataByDayRange(153208L, start, end, "5min"); //Temperature
//        instance.getResourceDataByDayRange(143844L, start, end, "5min"); // Calculated Power Consumption
//        instance.getResourceDataByDayRange(156983L, start, end, "hour"); // Relative Humidity
//        instance.getResourceDataByDayRange(147621L, start, end, "day");
//        instance.getResourceIdDetails(153208L,"property");
//        instance.getResourceLatest(153208L,"latestMonth");
//        instance.getResourceSummary(153208L,"max");
//        instance.getResourceDataByDayRange(153208L, start, end, "5min");

        System.out.println("Check out the resources data on getResourceDataByDayRange API");
        List<Long> siteIds = instance.listSitesIds();
        for (Long siteId : siteIds){
            List<Long> resourcesIds = instance.listResourcesIds(siteId);
            for (Long resourceId: resourcesIds){
                Matcher matcher = instance.getResourceDataByDayRange(resourceId, start, end, "day");
                List<Float> matches = new ArrayList<Float>();
                while (matcher.find()) {
                    String group = matcher.group();
//                    System.out.println(group);
                    String [] values = group.split(",");
                    float reading = Float.parseFloat(values[0].split("=")[1]);
                    String timestamp = values[1].split("=")[1].split("}")[0];
                    matches.add(reading);
                }
                System.out.println(matches);
            }
        }
    }
}

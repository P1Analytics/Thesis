package DataSource;
import java.io.FileWriter;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;

public class WriteSensorData {
    public static void main(String[] args) throws Exception {
        SparkAPI instance = new SparkAPI();
        instance.setUp();
        instance.authenticate();

        String freq = "5min"; //5min,hour,day,
        DateFormat time_format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        ZonedDateTime start = ZonedDateTime.parse("2016-09-01T00:00:00+01:00[Europe/Rome]");
        ZonedDateTime end = ZonedDateTime.parse("2017-01-01T00:00:00+01:00[Europe/Rome]");

        String folder = "/Users/nanazhu/Documents/Sapienza/Thesis/src_python/5min2016Sep-Dec/";
        List<Long> siteIds = Arrays.asList(
                144242L, 27827L, 144024L,
                155076L, 155849L, 155077L,
                155865L, 155877L, 28843L,
                144243L, 28850L, 159705L,
                157185L, 155851L, 19640L
        );
        for (Long siteId : siteIds) {
            System.out.println(siteId);
            FileWriter fw = new FileWriter(folder+siteId+".csv", true);
            for (Long subsiteId : instance.listSubsites(siteId)) {
                List<Long> sub_resourcesIds = instance.listResourcesIds(subsiteId);
                WriteResouceData(sub_resourcesIds,instance, freq, start, end, time_format,fw);
            }
            List<Long> resourcesIds = instance.listResourcesIds(siteId);
            WriteResouceData(resourcesIds,instance, freq,start, end,time_format,fw);
            fw.close();
        }
    }

    private static void WriteResouceData(List<Long> resourcesIds, SparkAPI instance, String freq,
                             ZonedDateTime start, ZonedDateTime end, DateFormat time_format,FileWriter fw) {
        for (Long resourceId : resourcesIds) {
            try{
                Matcher matcher = instance.getResourceDataByDayRange(resourceId, start, end, freq);
                while (matcher.find()) {
                    String group = matcher.group();
                    String [] values = group.split(",");
                    float value = Float.parseFloat(values[0].split("=")[1]);
                    String timestamp = values[1].split("=")[1].split("}")[0];
                    Timestamp ts=new Timestamp(Long.parseLong(timestamp));
                    String time = time_format.format(ts);
                    fw.write(resourceId + ";" + time + ";" + value + "\n");
                    System.out.println(resourceId + ";" + time +";" + value);
                }
            }
            catch (Exception e){
                System.out.println("ERROR: "+resourceId+" "+e );
            }
        }
    }

}




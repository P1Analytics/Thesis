package DataSink;

import DataSource.SparkAPI;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

public class WriteCassandra {

    public static void main (String[] args) throws Exception {

        System.out.println("this is to write the data ");
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        String keyspace = "GAIA";
        Session session = cluster.connect(keyspace);
        String table_name = "reading_data";
//        TableMetadata table = cluster.getMetadata().getKeyspace("GAIA").getTable("");

        SparkAPI instance = new SparkAPI();
        instance.setUp();
        instance.authenticate();

        ZonedDateTime end = ZonedDateTime.now();
        ZonedDateTime start = end.minusMonths(2);

        List<Long> siteIds = instance.listSitesIds();
        for (Long siteId : siteIds){
            List<Long> resourcesIds = instance.listResourcesIds(siteId);
            for (Long resourceId: resourcesIds){
                String freq = "hour";
                Matcher matcher = instance.getResourceDataByDayRange(resourceId, start, end, freq);
                List<Float> matches = new ArrayList<Float>();
                while (matcher.find()) {
                    String group = matcher.group();
                    String [] values = group.split(",");
                    float reading = Float.parseFloat(values[0].split("=")[1]);
                    String timestamp = values[1].split("=")[1].split("}")[0];
                    String query = "INSERT INTO "+keyspace+"."+ table_name
                            + " (ResourceID, Reading,Timestamp ) VALUES("
                            + resourceId+", "+ reading+", "+ timestamp+" );" ;
                    System.out.println(query);
                    session.execute(query);
                    matches.add(reading);
                }
                System.out.println(matches);
            }
        }
        cluster.close();
    }
}
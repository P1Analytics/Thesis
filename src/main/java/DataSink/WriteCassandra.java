package DataSink;

import DataSource.SparkAPI;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.regex.Matcher;

public class WriteCassandra {

    public static void main (String[] args) throws Exception {

        System.out.println("write the data ");
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        String keyspace = "GAIA";
        Session session = cluster.connect(keyspace);
        String table_name = "read_data";

        SparkAPI instance = new SparkAPI();
        instance.setUp();
        instance.authenticate();

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

        ZonedDateTime end = ZonedDateTime.now();
        ZonedDateTime start = end.minusMonths(30);
        String freq = "5min";

        List<Long> siteIds = instance.listSitesIds();
        for (Long siteId : siteIds){
            System.out.println(siteId);
            List<Long> resourcesIds = instance.listResourcesIds(siteId);
            for (Long resourceId: resourcesIds){
                try{
                    System.out.println(resourceId);
                    Matcher matcher = instance.getResourceDataByDayRange(resourceId, start, end, freq);
                    while (matcher.find()) {
                        String group = matcher.group();
                        String [] values = group.split(",");
                        float value = Float.parseFloat(values[0].split("=")[1]);
                        String timestamp = values[1].split("=")[1].split("}")[0];

                        Timestamp ts=new Timestamp(Long.parseLong(timestamp));
                        Date date=new Date(ts.getTime());
                        String dateS = df.format(date);

                        String query = "INSERT INTO " + keyspace + "." + table_name
                                + " (id,date,timeindex,value) VALUES("
                                + resourceId +", "+
                                "\'"+dateS + "\'" +", " + timestamp+", " + value+" );" ;
                        System.out.println(query);
                        session.execute(query);
                    }
                }
                catch (Exception e){
                    System.out.println("----------------------------------" + e + " on " + resourceId);
                }
            }

            List<Long> subsitesIds = instance.listSubsites(siteId);
            for (Long subsiteId : subsitesIds) {
                List<Long> sub_resourcesIds = instance.listResourcesIds(subsiteId);
                for (Long resourceId: sub_resourcesIds){
                    try{
                        System.out.println(resourceId);
                        Matcher matcher = instance.getResourceDataByDayRange(resourceId, start, end, freq);
                        while (matcher.find()) {
                            String group = matcher.group();
                            String [] values = group.split(",");
                            float value = Float.parseFloat(values[0].split("=")[1]);
                            String timestamp = values[1].split("=")[1].split("}")[0];

                            Timestamp ts=new Timestamp(Long.parseLong(timestamp));
                            Date date=new Date(ts.getTime());
                            String dateS = df.format(date);

                            String query = "INSERT INTO " + keyspace + "." + table_name
                                    + " (id,date,timeindex,value) VALUES ("
                                    + resourceId +", "+
                                    "\'"+dateS + "\'" +", " + timestamp+", " + value+" );" ;
                            System.out.println(query);
                            session.execute(query);
                        }
                    }
                    catch (Exception e){
                        System.out.println("----------------------------------" + e + " on " + resourceId);
                    }
                }
            }
        }
        cluster.close();
    }
}
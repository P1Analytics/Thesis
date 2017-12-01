package DataSource;
import DataSource.SparkAPI;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.List;

import static io.restassured.path.json.JsonPath.from;

public class WriteSensorInfo {
    public static void main(String[] args) throws Exception {
        SparkAPI instance = new SparkAPI();
        instance.setUp();
        instance.authenticate();

        String folder = "/Users/nanazhu/Documents/Sapienza/Thesis/src_python/details_resources/";
        List<Long> siteIds =instance.listSitesIds();
        for (Long siteId : siteIds) {
            FileWriter fw = new FileWriter(folder+siteId+".csv", true);
            System.out.println(siteId);
            for (Long subsiteId : instance.listSubsites(siteId)) {
                List<Long> resourcesIds = instance.listResourcesIds(subsiteId);
                WriteSensorDetails(subsiteId, resourcesIds, siteId, instance, fw);
            }

            List<Long> resourcesIds = instance.listResourcesIds(siteId);
            WriteSensorDetails(0L, resourcesIds, siteId, instance, fw);
            fw.close();
        }
    }
    static void WriteSensorDetails(Long subsiteId, List<Long> resourcesIds, Long siteId, SparkAPI instance, FileWriter fw) throws Exception {
        for (Long resourceId : resourcesIds) {
            String response  = (instance.getResourceIdDetails(resourceId,""));
            String uri = (String)from(response).get("uri");
            String prope = (String)from(response).get("property");
            fw.write(siteId + ";" +resourceId + ";" +subsiteId+";" + prope + ";" + uri+ "\n");
            System.out.println(siteId + ";" +resourceId + ";" +subsiteId+";" + prope + ";"  + uri);
        }
    }
}

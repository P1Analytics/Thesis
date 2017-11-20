package DataSource;

import java.util.List;

public class PrintAll {
    public static void main(String[] args) throws Exception {
        SparkAPI instance = new SparkAPI();
        instance.setUp();
        instance.authenticate();

        for (Long siteId : instance.listSitesIds()) {
            List<Long> resourcesIds = instance.listResourcesIds(siteId);
            System.out.println("**************************************"+siteId+"*****"+resourcesIds);
            for (Long resourceId : resourcesIds) {
                System.out.println(resourceId + ":" + instance.getResourceIdDetails(resourceId, "uri"));

            }
            List<Long> subsitesIds = instance.listSubsites(siteId);
            for (Long subsiteId : subsitesIds) {
                System.out.println("**************************************"+siteId+"*****"+subsitesIds);
                List<Long> sub_resourcesIds = instance.listResourcesIds(subsiteId);
                for (Long resourceId: sub_resourcesIds){
                    System.out.println(resourceId+":"+instance.getResourceIdDetails(resourceId,"uri"));

                }
            }
        }
    }
}

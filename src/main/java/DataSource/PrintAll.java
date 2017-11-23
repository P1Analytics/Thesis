package DataSource;

import java.util.List;

public class PrintAll {
    public static void main(String[] args) throws Exception {
        SparkAPI instance = new SparkAPI();
        instance.setUp();
        instance.authenticate();

        for (Long siteId : instance.listSitesIds()) {
//            siteId = 155077L;
            List<Long> resourcesIds = instance.listResourcesIds(siteId);
            System.out.println("**************************************"+siteId+"*****");
            for (Long resourceId : resourcesIds) {
                System.out.println(resourceId + ":" + instance.getResourceIdDetails(resourceId, "uri"));

                if (instance.getResourceIdDetails(resourceId,"property").contains("Temperature"))
                {
                    System.out.println(resourceId + ":" + instance.getResourceIdDetails(resourceId, "uri"));
                }
                if (instance.getResourceIdDetails(resourceId,"uri").contains("Power"))
                {
                    System.out.println(resourceId + ":" + instance.getResourceIdDetails(resourceId, "uri"));
                }
            }
            List<Long> subsitesIds = instance.listSubsites(siteId);
            for (Long subsiteId : subsitesIds) {
                System.out.println("**************************************"+siteId+"*****"+subsiteId);
                List<Long> sub_resourcesIds = instance.listResourcesIds(subsiteId);
                for (Long resourceId: sub_resourcesIds){
                    System.out.println(resourceId+":"+instance.getResourceIdDetails(resourceId,"uri"));

                    if (instance.getResourceIdDetails(resourceId,"property").contains("Power Consumption"))
                    {
                        System.out.println(resourceId+":"+instance.getResourceIdDetails(resourceId,"uri"));
                    }
                    if (instance.getResourceIdDetails(resourceId,"uri").contains("libelium"))
                    {
                        System.out.println(resourceId + ":" + instance.getResourceIdDetails(resourceId, "uri"));
                    }
                }
            }
        }
    }
}

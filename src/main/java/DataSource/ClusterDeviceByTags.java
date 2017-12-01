package DataSource;
import java.util.*;

public class ClusterDeviceByTags {
//    Turn URI as tag list
//    Map-reduce the tag with resource id to cluster them into same device
//    Output Dictionary List  [Key,Value] = [potential devices , set of resources on this device]

    public static void main(String[] args) throws Exception {
        SparkAPI instance = new SparkAPI();
        instance.setUp();
        instance.authenticate();

        List<Long> siteIds = instance.listSitesIds();
//        List<Long> siteIds = Arrays.asList(155076L,155077L,159705L);
        for (Long siteId :siteIds ) {
            List<Long> resourcesIds = instance.listResourcesIds(siteId);
            System.out.println("**************************************"+siteId);
            Map<List< String >, List<Long>> devices = new HashMap<>();
            for (Long resourceId : resourcesIds) {
                String[] response_uri = instance.getResourceIdDetails(resourceId,"uri").split("/");
                List< String > key = Arrays.asList(response_uri).subList(0,response_uri.length-1);
                // when dealing with italian system ... you know how messy it could be
                if (key.contains("gaia-prato")) {
                    key = Arrays.asList(response_uri).subList(0, 4);
                }
                if (key.contains("sapienza")) {
                    key = Arrays.asList(response_uri).subList(0, 3);
                }
//                System.out.println(key +":" + resourceId);
                if (devices.containsKey(key)){
                    devices.get(key).add(resourceId);
                }
                else{
                    devices.put(key, new ArrayList<>());
                    devices.get(key).add(resourceId);
                }
            }

            List<Long> subsitesIds = instance.listSubsites(siteId);
            for (Long subsiteId : subsitesIds) {
                List<Long> sub_resourcesIds = instance.listResourcesIds(subsiteId);
                for (Long resourceId: sub_resourcesIds){
                    String[] response_uri = instance.getResourceIdDetails(resourceId,"uri").split("/");
                    List< String > key = Arrays.asList(response_uri).subList(0,response_uri.length-1);
                    if (key.contains("gaia-prato")){
                        key = Arrays.asList(response_uri).subList(0,4);}
                    if (key.contains("sapienza")){
                        key = Arrays.asList(response_uri).subList(0,3);
                    }
//                    System.out.println(key +":" + resourceId);
                    if (devices.containsKey(key)){
                        devices.get(key).add(resourceId);
                    }
                    else{
                        devices.put(key, new ArrayList<>());
                        devices.get(key).add(resourceId);
                    }
                }
            }
            System.out.println(devices);
        }
    }
}

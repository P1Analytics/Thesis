package DataSource;

import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.path.json.JsonPath;
import io.restassured.path.json.config.JsonPathConfig;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.time.ZonedDateTime;

import static io.restassured.RestAssured.*;
import static io.restassured.path.json.JsonPath.from;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class SparkAPI {

    private final  String accessTokenUrl = "https://sso.sparkworks.net/aa/oauth/token";
    private final  String grantType ="password";
    private static String accessToken;
    private RequestSpecBuilder requestSpecBuilder;

    public void setUp() throws Exception {
        reset();
        baseURI =  "https://api.sparkworks.net/";
        basePath = "/v1";
        requestSpecBuilder = new RequestSpecBuilder();
        requestSpecBuilder.setAccept(ContentType.JSON);
    }

    private List<String> GetUserInfo() throws Exception {
        try{
            DataInputStream in = new DataInputStream(new FileInputStream("UserInfo"));
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String username = br.readLine();
            String password = br.readLine();
            String clientId = br.readLine();
            String clientSecret = br.readLine();
            in.close();
            List<String> UserInfo = Arrays.asList(username, password,clientId,clientSecret);
            return UserInfo;
        }catch (Exception e){
            System.err.println("Error: " + e.getMessage());
        }
        return null;
    }
    public void authenticate() throws Exception {
        useRelaxedHTTPSValidation();
        List<String> UserInfo = GetUserInfo();
        JsonPath.config = new JsonPathConfig("UTF-8");
        String response = given().accept(ContentType.JSON)
                .params("username", UserInfo.get(0), "password", UserInfo.get(1), "grant_type", grantType, "scope", "read")
                .auth().preemptive().basic(UserInfo.get(2), UserInfo.get(3))
                .when()
                .post(accessTokenUrl).asString();
        accessToken = new JsonPath(response).getString("access_token");
    }

    public List<Long> listSitesIds() throws Exception, AssertionError {
//        GET /v1/location/site Retrieve a collection of Sites
        String get_str = "/location/site";
        String response = given()
                .spec(requestSpecBuilder.build())
                .auth().preemptive().oauth2(accessToken)
                .get(get_str)
                .then().assertThat()
                .statusCode(200)
                .body("sites.size()", greaterThan(0))
                .extract()
                .body().asString();
        List<Long> siteIds = from(response).getList("sites.id", Long.class);

//      TODO  System.out.println("Here is the latitude and longtitude list: ");
//        List<String> sitesNames = from(response).getList("sites.name", String.class);
//        List<Float> siteslongtitude = from(response).getList("sites.longtitude", Float.class);
//        List<Float> siteslatitude = from(response).getList("sites.latitude", Float.class);
//        for (int i = 0;i<siteIds.size();i++){
//            System.out.println(Long.toString(siteIds.get(i))+"\t"+ sitesNames.get(i)+"\t"+siteslongtitude.get(i)+"\t"+siteslatitude.get(i));
//            System.out.println(Long.toString(siteIds.get(i))+"\t"+siteslongtitude.get(i)+"\t"+siteslatitude.get(i));
//        }
        return siteIds;
    }

    public List<Long> listResourcesIds(Long siteId) throws Exception, AssertionError {
//        GET /v1/location/site/{siteId}/resource Retrieve the Resources of a Site
        String get_str = "/location/site/" + Long.toString(siteId) + "/resource";
        String response = given()
                .spec(requestSpecBuilder.build())
                .auth().preemptive().oauth2(accessToken)
                .get(get_str)
                .then().assertThat()
                .statusCode(200)
                .extract()
                .body().asString();
        List<Long> resourceIds = from(response).getList("resources.resourceId", Long.class);
        return resourceIds;
    }

    public Long getSiteIdRandom(List<Long> siteIds) throws Exception, AssertionError {
//        GET /v1/location/site/{siteId} Retrieve Site by the site unique identifier
        Long siteId = siteIds.get(new Random().nextInt(siteIds.size()));
        System.out.println("CHECKOUT the siteId randomly " + siteId + " and also some details");

        String get_str = "/location/site/" + Long.toString(siteId);
        String response = given().spec(requestSpecBuilder.build())
                .auth().preemptive().oauth2(accessToken)
                .get(get_str)
                .then().assertThat()
                .statusCode(200)
                .body("id", equalTo(siteId.intValue()))
                .extract()
                .body().asString();
//        System.out.println(from(response).getList("subsites.user.username"));
        return siteId;
    }

    public List<Long>  listSubsites(Long siteId) throws Exception, AssertionError {
//        GET GET /v1/location/site/{siteId}/subsite Retrieve the subsite
        String get_str = "/location/site/" + Long.toString(siteId) + "/subsite";
        String response = given()
                .spec(requestSpecBuilder.build())
                .auth().preemptive().oauth2(accessToken)
                .get(get_str)
                .then().assertThat()
                .statusCode(200)
                .extract()
                .body().asString();
        List<Long> subsitesIds = from(response).getList("sites.id", Long.class);
        List<String> subsitesNames = from(response).getList("sites.name", String.class);
//        for (int i = 0;i<subsitesIds.size();i++){
//            System.out.println(Long.toString(subsitesIds.get(i))+"\t"+ subsitesNames.get(i));
//        }
        return subsitesIds;
    }

    public Long getResourceIdRandom(List<Long> resourceIds) throws Exception, AssertionError {
//        GET /v1/resource/{resourceId} Retrieve a Spark Works Resource Details by its unique identifier
        Long resourceId = resourceIds.get(new Random().nextInt(resourceIds.size()));
        System.out.println("CHECKOUT the resouceId randomly " + resourceId + " and check its properity");
        String get_str = "/resource/" + Long.toString(resourceId);
        String response = given().spec(requestSpecBuilder.build())
                .auth().preemptive().oauth2(accessToken)
                .get(get_str)
                .then().assertThat()
                .statusCode(200)
                .body("resourceId", equalTo(resourceId.intValue()))
                .extract()
                .body().asString();
        return resourceId;
    }

    public void getresouceIDTag(Long resourceId) throws Exception, AssertionError {
//      GET /v1/resource/{resourceId}/tag Retrieve the tags of a Resource by the resource unique identifier
        String get_str = "/resource/"+ Long.toString(resourceId)+"/tag";
        String response = given().spec(requestSpecBuilder.build())
                .auth().preemptive().oauth2(accessToken)
                .get(get_str)
                .then().assertThat()
                .statusCode(200)
                .extract()
                .body().asString();
        List<Long> data = from(response).getList("tag");
        System.out.println(data);
    }

    public String getResourceIdDetails(Long resourceId, String detail) throws Exception, AssertionError {
//        GET /v1/resource/{resourceId} Retrieve a Spark Works Resource Details by its unique identifier
        try{
//            System.out.println("CHECKOUT the resouceId " + resourceId + " with details "+ detail);
            String get_str = "/resource/" + Long.toString(resourceId);
            String response = given().spec(requestSpecBuilder.build())
                    .auth().preemptive().oauth2(accessToken)
                    .get(get_str)
                    .then().assertThat()
                    .statusCode(200)
                    .body("resourceId", equalTo(resourceId.intValue()))
                    .extract()
                    .body().asString();

            if (detail != "") { // avoid the nullpoint from sepcific items when retrieving the type of sensor
                return (String)from(response).get(detail);
            }
            return response;
        }
        catch(Exception e){
            System.out.println("getResourceIdDetails " + resourceId + " " + e);
            return null;
        }
    }

    public void getResourceLatest(Long resourceId,String latest_item) throws Exception,AssertionError {
//        GET /v1/resource/{resourceId}/latest Retrieve latest values of a Resource
        try {
            String get_str = "/resource/" + Long.toString(resourceId) + "/latest";
            String response = given()
                    .spec(requestSpecBuilder.build())
                    .auth().preemptive().oauth2(accessToken)
                    .get(get_str)
                    .then().assertThat()
                    .statusCode(200)
                    .body("id", equalTo(resourceId.intValue()))
                    .extract()
                    .body().asString();
            String latestValue = from(response).get(latest_item).toString();
            System.out.println("Resource " + resourceId + " latest value: " + latestValue);
        }
        catch(AssertionError e){
            System.out.println("getResourceLatest " + resourceId + " " + e);
        }
    }

    public String getResourceSummary(Long resourceId,String frequency) throws Exception,AssertionError {
//        GET /v1/resource/{resourceId}/summary Retrieve latest summary values of a Resource
//        GET /v1/resource/{resourceId}/summary/{targetUom} Retrieve latest summary values of a Resource
//        TODO same out for adding Uom with ID
        try {
            String get_str = "/resource/" + Long.toString(resourceId) + "/summary";
            String response = given()
                    .spec(requestSpecBuilder.build())
                    .auth().preemptive().oauth2(accessToken)
                    .get(get_str)
                    .then().assertThat()
                    .statusCode(200)
                    .extract()
                    .body().asString();
            if ("min" == frequency || "max" == frequency || "average" == frequency) {
                Map<String, Integer> data = from(response).getMap(frequency);
//                System.out.println("Resource " + resourceId + " in  \"" + frequency + "\": " + data);
            } else {
                List<Long> data = from(response).getList(frequency);
//                System.out.println("Resource " + resourceId + " in  \"" + frequency + "\": " + data);
            }
            String id = from(response).get("keyName").toString().split("/")[1];
            return id;
        }
        catch(AssertionError e){
            System.out.println("getResourceSummary " + resourceId + " " + e);
            return null;
        }
    }

    public Matcher getResourceDataByDayRange(Long resourceId, ZonedDateTime start, ZonedDateTime end, String frequency)
            throws Exception,AssertionError {
//        System.out.println("Resource  historical data resource ID: "+ resourceId +" from : " +  start.toInstant()+" to "
//                +  end.toInstant() + " with steps per " + frequency );
        String requestBody = "" +
                "{" +
                "\"queries\": [" +
                "{" +
                "\"from\": " + start.toInstant().toEpochMilli()/300000*300000 + "," +
                "\"granularity\": \""+ frequency + "\"," +
                "\"resourceID\": " + resourceId + "," +
                "\"resultLimit\": 0," +
                "\"to\": " + end.toInstant().toEpochMilli()/300000*300000 + "" +
                "}" +
                "]" +
                "}";
        String get_str = "/resource/query/timerange";
        try{
            String response = given()
                    .spec(requestSpecBuilder.build())
                    .auth().preemptive().oauth2(accessToken)
                    .contentType(ContentType.JSON)
                    .body(requestBody)
                    .post(get_str)
                    .then().assertThat()
                    .statusCode(200)
                    .extract()
                    .body().asString();
            String data =from(response).get("results.values()").toString().split("=\\[")[1];
            data = data.substring(0, data.length() - 3);
//            int data_size = (int) (ChronoUnit.DAYS.between(start.toInstant(), end.toInstant()) + 1);
            Matcher matcher = Pattern.compile("\\{(.*?)\\}").matcher(data);
            return matcher;
//            List<Float> matches = new ArrayList<Float>(data_size);
//            while (matcher.find()) {
//                String group = matcher.group();
//                String [] values = group.split(",");
//                float reading = Float.parseFloat(values[0].split("=")[1]);
//                String timestamp = values[1].split("=")[1].split("}")[0];
////                System.out.println(group);
////                System.out.println(reading);
//                matches.add(reading);
//            }
//            System.out.println(matches);
////            System.out.println(matches.size());
        }
        catch (AssertionError e){
            System.out.println("getResourceDataByDayRange " +resourceId + " " + e);
            return null;
        }
    }
}
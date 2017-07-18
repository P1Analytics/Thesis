import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.path.json.JsonPath;
import io.restassured.path.json.config.JsonPathConfig;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.restassured.RestAssured.*;
import static io.restassured.path.json.JsonPath.from;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class RetrieveData {

    private final  String accessTokenUrl = "https://sso.sparkworks.net/aa/oauth/token";

    private final  String clientId = "spark";

    private final  String clientSecret = "spark";

    private final  String grantType ="password";

    private final  String username = "ro_schools";

    private final  String password="Readonly1234";

    private static String accessToken;

    private RequestSpecBuilder requestSpecBuilder;

    public void setUp() throws Exception {
        reset();
        baseURI =  "https://api.sparkworks.net/";
        basePath = "/v1";
        requestSpecBuilder = new RequestSpecBuilder();
        requestSpecBuilder.setAccept(ContentType.JSON);
    }

    private void authenticate() throws Exception {
        useRelaxedHTTPSValidation();
        // todo internet check
        JsonPath.config = new JsonPathConfig("UTF-8");
        String response = given().accept(ContentType.JSON)
                .params("username", username, "password", password, "grant_type", grantType, "scope", "read")
                .auth().preemptive().basic(clientId, clientSecret)
                .when()
                .post(accessTokenUrl).asString();
        accessToken = new JsonPath(response).getString("access_token");
        System.out.println("Access_token: "+ accessToken);
    }

    private Long getResourceIdRandom(List<Long> resourceIds) throws Exception, AssertionError {
        Long resourceId = resourceIds.get(new Random().nextInt(resourceIds.size()));
        System.out.println("CHECKOUT the resouceId randomly " + resourceId);

        String get_str = "/resource/" + Long.toString(resourceId);
        String response = given().spec(requestSpecBuilder.build())
                .auth().preemptive().oauth2(accessToken)
                .get(get_str)
                .then().assertThat()
                .statusCode(200)
                .body("resourceId", equalTo(resourceId.intValue()))
                .extract()
                .body().asString();
        System.out.println(from(response).get("property"));
        return resourceId;
    }

    private Long getSiteIdRandom(List<Long> siteIds) throws Exception, AssertionError {
        Long siteId = siteIds.get(new Random().nextInt(siteIds.size()));
        System.out.println("CHECKOUT the siteId randomly " + siteId);

        String get_str = "/location/site/" + Long.toString(siteId);
        given().spec(requestSpecBuilder.build())
                .auth().preemptive().oauth2(accessToken)
                .get(get_str)
                .then().assertThat()
                .statusCode(200)
                .body("id", equalTo(siteId.intValue()))
                .extract()
                .body().asString();
        return siteId;
    }

    private List<Long> listSitesIds() throws Exception, AssertionError {
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
        System.out.println("CHECKOUT the siteIds list " + siteIds);
        
        return siteIds;
    }

    private List<Long> listResourcesIds(Long siteId) throws Exception, AssertionError {
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
        System.out.println(" CHECKOUT site " + siteId + " the resourcesIds list " + resourceIds);

        return resourceIds;
    }

    private void getResourceIdProperty(Long resourceId , String property) throws Exception, AssertionError {
        String get_str = "/resource/" + Long.toString(resourceId);
        String response = given().spec(requestSpecBuilder.build())
                .auth().preemptive().oauth2(accessToken)
                .get(get_str)
                .then().assertThat()
                .statusCode(200)
                .body("resourceId", equalTo(resourceId.intValue()))
                .extract()
                .body().asString();
        System.out.println("resouceId "+ resourceId + " : " + from(response).get(property));
    }

    private void getResourceLatestData(Long resourceId) throws Exception {
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
        String latestValue = from(response).get("latest").toString();
        System.out.println("Resource "+ resourceId + " latest value: " + latestValue);
    }
    
    private void getResourceSummary(Long resourceId) throws Exception {
        String get_str = "/resource/"+ Long.toString(resourceId)+"/summary";
        String response = given()
                .spec(requestSpecBuilder.build())
                .auth().preemptive().oauth2(accessToken)
                .get(get_str)
                .then().assertThat()
                .statusCode(200)
                .extract()
                .body().asString();
        List<Long> data = from(response).getList("minutes60");
        System.out.println("Resource "+ resourceId+ " in  \"minutes60\": "+ data);
    }

    private void getResourceDataByDayRange(Long resourceId, ZonedDateTime start, ZonedDateTime end, String frequency) {
        System.out.println("Resource  historical data resource ID: "+ resourceId +" from : " +  start.toInstant()+" to "
                +  end.toInstant() + " with steps per " + frequency );
        String requestBody = "" +
                "{" +
                "\"queries\": [" +
                "{" +
                "\"from\": " + start.toInstant().toEpochMilli() + "," +
                "\"granularity\": \""+ frequency + "\"," +
                "\"resourceID\": " + resourceId + "," +
                "\"resultLimit\": 0," +
                "\"to\": " + end.toInstant().toEpochMilli() + "" +
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

            int data_size = (int) (ChronoUnit.DAYS.between(start.toInstant(), end.toInstant()) + 1);

            Matcher matcher = Pattern.compile("\\{(.*?)\\}").matcher(data);

            List<String> matches = new ArrayList<String>(data_size);
            while (matcher.find()) {
                String group = matcher.group();
                System.out.println(group);
                matches.add(group);
            }
//            String[] groups = matches.toArray(new String[matches.size()]);
//            System.out.println(Arrays.toString(groups));
        }
        catch (AssertionError e){
            System.out.println("Nothing available on "+ resourceId);

        }
    }

    public static void main (String[] args) throws Exception{
        RetrieveData instance = new RetrieveData();
        instance.setUp();
        instance.authenticate();

        ZonedDateTime start = ZonedDateTime.now().minusWeeks(1);
        ZonedDateTime end = ZonedDateTime.now();

        List<Long> siteIds = instance.listSitesIds();

        // looping  trial
        System.out.println("Check out the resources data");
        for (Long siteId : siteIds){
            List<Long> resourcesIds = instance.listResourcesIds(siteId);
            for (Long resourceId: resourcesIds){
                instance.getResourceLatestData(resourceId);
                instance.getResourceSummary(resourceId);
                instance.getResourceIdProperty(resourceId,"property");
                instance.getResourceDataByDayRange(resourceId, start, end, "day");
            }
        }

        // SINGLE trial
//        Long siteId = instance.getSiteIdRandom(siteIds);
//        List<Long> resourcesIds = instance.listResourcesIds(siteId);
//        Long resourceId = instance.getResourceIdRandom(resourcesIds);
//        System.out.println("Following is the data we monitored");
//        instance.getResourceLatestData(resourceId);
//        instance.getResourceSummary(resourceId);
//        instance.getResourceIdProperty(resourceId,"property");
//        instance.getResourceDataByDayRange(resourceId, start, end, "day");
    }
}
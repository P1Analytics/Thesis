package DataSink;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.path.json.JsonPath;
import io.restassured.path.json.config.JsonPathConfig;

import java.io.FileWriter;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.restassured.RestAssured.*;
import static io.restassured.path.json.JsonPath.from;

public class Cassandra {
    private final  String accessTokenUrl = "https://sso.sparkworks.net/aa/oauth/token";
    private final  String grantType ="password";

    private final  String clientId = "spark";
    private final  String clientSecret = "spark";

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
        JsonPath.config = new JsonPathConfig("UTF-8");
        String response = given().accept(ContentType.JSON)
                .params("username", username, "password", password, "grant_type", grantType, "scope", "read")
                .auth().preemptive().basic(clientId, clientSecret)
                .when()
                .post(accessTokenUrl).asString();
        accessToken = new JsonPath(response).getString("access_token");
        System.out.println("Access_token: "+ accessToken);
    }

    private List<String> getResourceDataByDayRange(Long resourceId, ZonedDateTime start, ZonedDateTime end, String frequency) throws Exception,AssertionError {

        String query = "";
        List<String> queries = new ArrayList<String>();
        System.out.println("Resource  historical data resource ID: "+ resourceId +" from : " +  start.toInstant()+" to "
                +  end.toInstant() + ",delta +/- 5mins, with steps per " + frequency );
        String requestBody = "" +
                "{" + "\"queries\": [" + "{" +
                "\"from\": " + start.toInstant().toEpochMilli() + "," +
                "\"granularity\": \""+ frequency + "\"," +
                "\"resourceID\": " + resourceId + "," +
                "\"resultLimit\": 0," +
                "\"to\": " + end.toInstant().toEpochMilli() + "" +"}" +"]" +"}";
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
            Matcher matcher = Pattern.compile("\\{(.*?)\\}").matcher(data);

            String file_name = resourceId+"_"+frequency+".csv";
            FileWriter fw = new FileWriter(file_name,true);

            int data_size = (int) (ChronoUnit.DAYS.between(start.toInstant(), end.toInstant()) + 1);
            ArrayList<Float> matches = new ArrayList<Float>(data_size);
            while (matcher.find()) {
                String group = matcher.group();
                String [] values = group.split(",");
                float reading = Float.parseFloat(values[0].split("=")[1]);
                String timestamp = values[1].split("=")[1].split("}")[0];

                fw.write(group+"\n");

                query = "INSERT INTO gaia.reading_data  (ResourceID, Reading,Timestamp ) VALUES("
                        + resourceId+", "+ reading+", "+ timestamp+" );" ;
                queries.add(query);

                matches.add(reading);
                System.out.println(query);
            }
            fw.close();
            System.out.println("print out the matches :");
            System.out.println(matches);
            try{
                String[] groups = matches.toArray(new String[matches.size()]);
                System.out.println(Arrays.toString(groups));
            }
            catch (ArrayStoreException e){
                System.out.println(e.getMessage());
            }
        }
        catch (AssertionError e){
            System.out.println("getResourceDataByDayRange " +resourceId + " " + e);
        }
        return queries;
    }

    public static void main (String[] args) throws Exception {
        Cassandra instance = new Cassandra();
        instance.setUp();
        instance.authenticate();
        ZonedDateTime start = ZonedDateTime.now().minusMonths(3);
        ZonedDateTime end = ZonedDateTime.now();

        System.out.println("this is to write the data ");


        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        Session session = cluster.connect("GAIA");
        TableMetadata table = cluster.getMetadata().getKeyspace("GAIA").getTable("");
//        if (table.exportAsString() == "null"){
//            System.out.println("No table(s)");
//        }

        List<String> queries = instance.getResourceDataByDayRange(157221L, start, end, "hour"); //Temperature

        for (String query : queries){
            System.out.println(query);
            session.execute(query);
        }
        cluster.close();


    }
}
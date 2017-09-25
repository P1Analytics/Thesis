
# Thesis
    This will be a story about "data"

## Known Issues:
- [x] when we request data within the time range, for different granularity , 
 the response time stamps are different 
     - 5min : it's the code running time, not the fixed data timestamp as below
     - 1hour : beginning of every hour 
     - 1day : 21:00 for each day 
     - 1month :  at 21:00 last day of the month
    
     __Solution__ : use ".toInstant().toEpochMilli()/ 300000*300000" to change the time into 5mins interval in one hour. 
     So we will have 5', 10',15',...55' for the record timestamp

     
- [x] Cassandra do not have connector as data source for flink
    
    __Solution__
    
    [reference : CassandraConnectorITCase ](https://github.com/apache/flink/blob/master/flink-connectors/flink-connector-cassandra/src/test/java/org/apache/flink/streaming/connectors/cassandra/CassandraConnectorITCase.java)
    ```
    ClusterBuilder cb = new ClusterBuilder() {
            @Override
            public Cluster buildCluster(Cluster.Builder builder) {
                return builder.addContactPoint("127.0.0.1").build();
            }
        };
    String query = "SELECT ResourceID,Reading FROM gaia.reading_data WHERE ResourceID=155873";
    InputFormat<Tuple2<Integer, Float>, InputSplit> source = new CassandraInputFormat<>(query, cb);
    source.configure(null);
    source.open(null);
    List<Tuple2<Integer, Float>> result = new ArrayList<>();
    while (!source.reachedEnd()) result.add(source.nextRecord(new Tuple2<Integer, Float>()));
    source.close();
    ```
    
- [x] Can't read data through API: 

    Resource  historical data resource ID: 90946 from : 2017-08-24T09:25:49.323Z to 2017-08-31T09:25:49.080Z with steps per hour
[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0] 
However the real data is not all-zeros in https://console.sparkworks.net/resource/view/90946
    
    We will use the data extracted from API for further process , the console data are only for reference.
    
- Data is missed from time to time: 

    Resource  historical data resource ID: 155918 from : 2017-08-24T09:25:49.323Z to 2017-08-31T09:25:49.080Z with steps per hour [32.34, 32.34, 32.34, 32.473637, 33.4425, 34.259167, 34.75733, 34.365334, 33.32, 32.764668, 32.570587, 32.36722, 32.3155, 32.3068, 32.241306, 31.868149, 31.868149, 31.85, 31.838118, 31.808867, 31.826338, 31.822662, 31.832302, 31.852188, 31.842134, 31.841246, 31.85319, 32.140522, 33.013374, 33.919827, 34.664608, 34.72307, 32.34, 32.34, 32.34, 31.85, 31.85, 32.34, 31.85, 31.85, 31.85, 31.85, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 31.367912, 31.374935, 31.410751, 31.416487, 31.51673, 31.94285, 32.77366, 33.64148, 34.232605, 33.768044, 32.68922, 32.30267, 32.140594, 31.958946, 31.87191, 31.852951, 31.830269, 31.64, 31.46889, 31.456898, 31.3657, 31.385254, 31.381018, 31.41322, 31.534771, 31.668463, 31.79291, 31.836, 31.869188, 32.38108, 33.044456, 33.666306, 33.827496, 32.96945, 32.38393, 32.23478, 32.038074, 31.907423, 31.856163, 31.844433, 31.838556, 31.69923, 31.6932, 31.511343, 31.412098, 31.391008, 31.380096, 31.443954, 31.619139, 31.776262, 31.830036, 31.832235, 31.799591, 31.84828, 32.09569, 32.928936, 33.33732, 33.126163, 32.188957, 31.83223, 31.623442, 31.422161, 31.361742, 31.356749, 31.3123, 30.906296, 30.886333, 30.878448, 30.859062, 30.843264, 30.859325, 30.861336, 30.849495, 30.85837, 30.85697]
    
    **TODO** We will need to identify the "noise" from data
    
## "Gazing at" the data
      When you gaze long into an abyss, the abyss also gazes into you
__What do these sensors collect?__

- Power consumption

    - Calculated Power Consumption : mWh
    - Power Consumption : mWh
    - Electrical Current : mA/A
    - Active Power : mW
    - Apparent Energy : Vah
    - Apparent Power : VA
    - Voltage : V
    
- Environmental parameters
    - Light : lux
    - Noise : Raw Value
    - Motion : Raw Value
    - Movement : Raw Value
    - Luminosity : Raw Value
    - External Air Contaminants : Raw Value
    - External Ammonia Concentration : Raw Value
    - External Carbon Dioxide Concentration : Raw Value
    - External Carbon Monoxide Concentration : Raw Value
    - External Oxygen Concentration : Raw Value
    - Carbon Monoxide Concentration : Raw Value
    - Methane Concentration : Raw Value
    - Power Factor : Raw Value
    - Atmospheric Pressure : kPa
    - Radiation ：uSv/h
    - Reactive Energy : VARh
    - Reactive Power : VAR
    
    - External Relative Humidity : %
    - Relative Humidity :  %
    - Rain Height : mm
    
    - Wind Direction : degrees
    - Wind Speed : m/sec
    - Temperature : Centigrade
    - External Temperature : C


- Demo on all the schools in Greece for one year, time interval: day
    - Demo for 10 schools in Greece for one year Power Consumption 
    ![greece one year](./image/power_greece_16Sep_17_Sep_perday.png?raw=true "")
    There are 3 other schools without power consumption sensors or no data.
    
    - Demo for 12 schools in Greece for one year Temperature 
      ![greece one year](./image/Greece%20Temperature.png?raw=true "")
        
- Demo on site __8ο Γυμνάσιο Πατρών,Greece__ ,for 3 weeks, time interval: hour        
    - Temperature 
        ![3 weeks temperature](./image/8ο%20Γυμνάσιο%20Πατρών%20Temperature.png?raw=true "")
        
    - Calculated Power Consumption 
        ![3 weeks Calculated Power Consumption](./image/8ο%20Γυμνάσιο%20Πατρών%20Calculated%20Power%20Consumption.png?raw=true "")
    
    - Main building Motion 
        ![3 weeks motion](./image/8ο%20Γυμνάσιο%20Πατρών%20Motion.png?raw=true"")
     
    - Subsite building(Αίθουσα ισόγειο) Motion
        ![3 weeks motion ](./image/Mo2.png?raw=true"")
    
    - Temperature for 4 weeks in the main building with building floor plan 
        ![4 weeks temperature](./image/27827%20Temperature1.png?raw=true"")
            
    
- Demo on site __Γυμνάσιο Πενταβρύσου Καστοριάς,Greece__, for 4 weeks, time interval: hour  
    - Temperature at the main building with building floor plan
    ![4 weeks Temperature](./image/19640%20Temperature0.png?raw=true"")

    - Temperature inside & outside of the building
    ![4 weeks Temperature](./image/19640%20Temperature3.png?raw=true"")
    
    - Calculated Power Consumption at the main building 
    ![4 weeks Calculated Power Consumption](./image/19640%20Calculated%20Power%20Consumption.png?raw=true"")
    
    - Electrical Current at the main building 
    ![4 weeks ElectricalCurrent](./image/19640%20ElectricalCurrent.png?raw=true"")
        
    - Motion at the main building 
    ![4 weeks motion](./image/19640%20motion1.png?raw=true"")
    
    - Luminosity at the main building 
    ![4 weeks Luminosity](./image/19640%20Luminosity1.png?raw=true"")
        
    - Noise at the main building 
    ![4 weeks motion](./image/19640%20noise1.png?raw=true"")
        
    - Rain Height and Relative Humidity at the main building
    ![4 weeks Rain Height and Relative Humidity](./image/19640%20RainHeight_humidity.png?raw=true"")

    - Motion at the sub-site building
    ![4 weeks motion](./image/19640%20motion2.png?raw=true"")
    
    - Motion at the sub-site building
    ![4 weeks Luminosity](./image/19640%20Luminosity2.png?raw=true"")
        
    - Noise at the sub-site building
    ![4 weeks motion](./image/19640%20noise2.png?raw=true"")
    
    - Motion and Noise at the sub-site building
    ![4 weeks motion](./image/19640%20subsite%20motion%20noise.png?raw=true"")

    - Temperature at the sub-site building
    ![4 weeks Temperature](./image/19640%20Temperature2.png?raw=true"")
            
    - Relative Humidity at the sub-site building
    ![4 weeks Relative Humidity](./image/19640%20humidity.png?raw=true"")
    
Demo on site __Δημοτικό Σχολείο Μεγίστης,Greece__, for 4 weeks, time interval: hour  
    - Temperature inside and outside of the main building with building floor plan
    ![4 weeks Temperature](./image/144243Temperature.png?raw=true"")
    
**TODO** More visualization for the data comparing with the ground truth 

__Where are these data from?__

 - Locations in [Map](https://drive.google.com/open?id=1MP6JIzob6P2g3Kvq-l-yRYSZAXE&usp=sharing)

| Country  | Parameter | Number | Comment | 
| ------------- | ------------- | ------------- | ------------- |
| Greece  | Sensing endpoints | 872 | Each sensor equals 1 sensing endpoint | 
| | Sensing rate | 1 minute | Can be modified | 
| | Educators | 294 | Greek public schools in GAIA| 
| |Students | 2267 | Greek public schools in GAIA | 
| Italy (Sapienza,Roma) | Sensing endpoints  | 118 |Will soon be augmented|
| | Sensing rate | 1 minute | Can be modified | 
| | Educators | 120 | University faculty and Post Doc |
| | Students | 1706 | University students | 
|Italy(Prato)|Sensing endpoints |117 |
| Sweden(Soderhamn)|Sensing endpoints |3 | create on 2017-07-12T12 more to add, no data yet| 
Total : 16 sites and 1922 sensors are on the record till 2017/09/16.
Part of them are newly installed in this year and some have history data from 2015.

__GAIA Architecture design overview__
 ![archetecture](./image/GAIAarchitecturedesignOverview.jpg?raw=true "")

__Cloud platform__
 ![cloud](./image/GAIAcloudplatform.png?raw=true "")


## Setup enviroment on mac OS Sierra 10
### Cassandra and Flink 
```
`brew install python` (may or maybe not need to use it but I run this anyway)

`pip install cql --ignore-installed six`

   --ignore-installed six is due to [fail of updating six on macOS 10.11](https://github.com/donnemartin/haxor-news/issues/54)
    
`brew install cassandra`
    
`brew install apache-flink`
```
### Why use Flink and Cassandra 

1. __Flink__

    Flink is well-designed on processing [streaming data](https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/datastream_api.html) 
    comparing with Spark, Hadoop.
    
    That is the crucial requisition for processing the sensor data
    
    Flink also has [connector for Cassandra as data sink](https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/connectors/index.html)
    and we can customize for connecting Cassandra as batch data source.
    
    ![architechture of flink](./image/flink.png?raw=true "")
    

2. __Cassandra__

    We are dealing with sensors data, which means a lot of machines, continuous emission and a large amount of data
    
    And Cassandra is one of the many options.
    
    Here are the [Key Cassandra Features and Benefits](https://academy.datastax.com/resources/brief-introduction-apache-cassandra)
    - Massively scalable architecture – a masterless design where all nodes are the same, which provides operational simplicity and easy scale-out.
    - Flexible and dynamic data model – supports modern data types with fast writes and reads.
    - Linear scale performance – the ability to add nodes without going down produces predictable increases in performance
    - Transparent fault detection and recovery – nodes that fail can easily be restored or replaced.
    - Multi-data center replication – cross data center (in multiple geographies) and multi-cloud availability zone support for writes/reads.
    - CQL (Cassandra Query Language) – an SQL-like language that makes moving from a relational database very easy.  
    - ...
    
    Allow me highlight the part we are looking for and give us more confidence:
    > __Top Use Cases__
    _While Cassandra is a general purpose non-relational database that can be used for a variety of different applications, there are a number of use cases where the database excels over most any other option._
    >> Internet of things applications – Cassandra is perfect for consuming lots of fast incoming data from devices, __sensors__ and similar mechanisms that exist in many different locations.
    
    There are a tons of [lectures how to use it properly](https://academy.datastax.com/courses)

    __Challenge__:
    - Unlike in the SQL world where you model your data first and then write the queries, in Cassandra you need to figure out all the queries that will be done and model your data accordingly.
    It means you need to think twice when [building the data model](https://medium.com/@jscarp)
    Now we just use a very simple table as below for current phase
    
    | ResourceID  | Reading | timestamp | 
    | ----------  | --------| ----------| 
    

(**_TODO_** more combinations of infrastructure MongoDB, MariaDB, Spark, Storm, Hadoop, Kafka, etc will be put on the trial list)
    
## Retrieve the data 
### Stream

- [x] Retrieve the data by using APIs on  https://api.sparkworks.net/swagger-ui.html
  
  Demo for "POST /v1/resource/query/timerange" : 
```  
    Command line:
        curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json' --header 'Authorization: Bearer cd885cf5-7fca-4be8-b32e-97225da6763f' -d '{
          "queries": [
            {
              "from": 1498867200000,
              "granularity": "day",
              "resourceID": 156972,
              "resultLimit": 0,
              "to": 1500076799000
            }
          ]
        }' 'https://api.sparkworks.net/v1/resource/query/timerange'
        
        
    Reponse body:
        {
          "results": {
            "{\"resourceID\":156972,\"resourceURI\":\"gaia-ea/room-1/temp\",\"from\":1498867200000,\"to\":1500076799000,\"granularity\":\"day\",\"resultLimit\":0}": {
              "average": 31.995042261495424,
              "summary": 479.92563392243136,
              "data": [
                {
                  "timestamp": 1498856400000,
                  "reading": 34.19535065107274
                },
                {
                  "timestamp": 1498942800000,
                  "reading": 35.618584889499054
                },
                {
                  "timestamp": 1499029200000,
                  "reading": 36.04129544661687
                },
                {
                  "timestamp": 1499115600000,
                  "reading": 33.757383570057335
                },
                {
                  "timestamp": 1499202000000,
                  "reading": 33.53155289889761
                },
                {
                  "timestamp": 1499288400000,
                  "reading": 33.69297495797249
                },
                {
                  "timestamp": 1499374800000,
                  "reading": 33.86900649403725
                },
                {
                  "timestamp": 1499461200000,
                  "reading": 34.38513502358479
                },
                {
                  "timestamp": 1499547600000,
                  "reading": 33.115313475098816
                },
                {
                  "timestamp": 1499634000000,
                  "reading": 35.82894403139205
                },
                {
                  "timestamp": 1499720400000,
                  "reading": 28.39513749980371
                },
                {
                  "timestamp": 1499806800000,
                  "reading": 26.729258248353453
                },
                {
                  "timestamp": 1499893200000,
                  "reading": 26.55955035714077
                },
                {
                  "timestamp": 1499979600000,
                  "reading": 25.807027188020786
                },
                {
                  "timestamp": 1500066000000,
                  "reading": 28.399119190883642
                }
              ]
            }
          }
        }
```

### Batch

## Process the data on Flink
__Target__ : 
- Visualize and Identify the user patten
- Predict abnormal case 
- Data ETL , including matching the ground truth.  


Flink has the special classes DataSet and DataStream to represent data in a program. 
You can think of them as immutable collections of data that can contain duplicates. 
In the case of DataSet the data is finite while for a DataStream the number of elements can be unbounded.

Flink program programs look like regular programs that transform collections of data. 
Each program consists of the same basic parts:
- Obtain an execution environment,
- Load/create the initial data,
- Specify transformations on this data,
- Specify where to put the results of your computations,
- Trigger the program execution

Let's talk something about "Key" first
- Define keys for Tuples 
- Define keys using Field Expressions
- Define keys using Key Selector Functions  
### Stream
- Reference [SocketWindowWordCount.java](https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/socket/SocketWindowWordCount.java)
### Batch 




## Reference
1. [The building data genome project](https://github.com/buds-lab/the-building-data-genome-project)
2. [Big data computing](https://piazza.com/uniroma1.it/spring2017/1044406/resources) 
3. [Naked Statistics: Stripping the Dread from the Data by Charles Wheelan](https://www.goodreads.com/book/show/17986418-naked-statistics)

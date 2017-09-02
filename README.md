# Thesis
    This will be a story about "data"

#### Known Issues:
- [x] when we request data within the time range, for different granularity , 
 the response time stamps are different 
     - 5min : it's the code running time, not the fixed data timestamp as below
     -----__walk around solution__ : use ".toInstant().toEpochMilli()/ 300000*300000" to change the time into 5mins interval
     - 1hour : beginning of every hour 
     - 1day : 21:00 for each day 
     - 1month :  at 21:00 last day of the month

- APIs options from Apache Flink 
    - MongoDB (customized need to try the functions?)
    - MariaDB (API for SQL write and read?) 
    - Hadoop (API for both write and read) 
    - Cassandra (API for write)

- Can't read data through API: 

    Resource  historical data resource ID: 90946 from : 2017-08-24T09:25:49.323Z to 2017-08-31T09:25:49.080Z with steps per hour
[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0] 
However the real data is not all-zeros in https://console.sparkworks.net/resource/view/90946
    
- Data is missed from time to time: 

    Resource  historical data resource ID: 155918 from : 2017-08-24T09:25:49.323Z to 2017-08-31T09:25:49.080Z with steps per hour [32.34, 32.34, 32.34, 32.473637, 33.4425, 34.259167, 34.75733, 34.365334, 33.32, 32.764668, 32.570587, 32.36722, 32.3155, 32.3068, 32.241306, 31.868149, 31.868149, 31.85, 31.838118, 31.808867, 31.826338, 31.822662, 31.832302, 31.852188, 31.842134, 31.841246, 31.85319, 32.140522, 33.013374, 33.919827, 34.664608, 34.72307, 32.34, 32.34, 32.34, 31.85, 31.85, 32.34, 31.85, 31.85, 31.85, 31.85, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 31.367912, 31.374935, 31.410751, 31.416487, 31.51673, 31.94285, 32.77366, 33.64148, 34.232605, 33.768044, 32.68922, 32.30267, 32.140594, 31.958946, 31.87191, 31.852951, 31.830269, 31.64, 31.46889, 31.456898, 31.3657, 31.385254, 31.381018, 31.41322, 31.534771, 31.668463, 31.79291, 31.836, 31.869188, 32.38108, 33.044456, 33.666306, 33.827496, 32.96945, 32.38393, 32.23478, 32.038074, 31.907423, 31.856163, 31.844433, 31.838556, 31.69923, 31.6932, 31.511343, 31.412098, 31.391008, 31.380096, 31.443954, 31.619139, 31.776262, 31.830036, 31.832235, 31.799591, 31.84828, 32.09569, 32.928936, 33.33732, 33.126163, 32.188957, 31.83223, 31.623442, 31.422161, 31.361742, 31.356749, 31.3123, 30.906296, 30.886333, 30.878448, 30.859062, 30.843264, 30.859325, 30.861336, 30.849495, 30.85837, 30.85697]
    
## "Staring at" the data
what do these sensors collect?

- Power consumption

    - Calculated Power Consumption : mWh
    - Power Consumption : mWh
    - Electrical Current : mA

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
    - Atmospheric Pressure : kPa
    - Radiation ： uSv/h
    
    - External Relative Humidity : %
    - Relative Humidity :  %
    - Rain Height : mm
    
    - Wind Direction : degrees
    - Wind Speed : m/sec
    - Temperature : Centigrade
    - External Temperature : C

Demo for sensor data on [SITE_ID 144242 with some correlations](https://drive.google.com/open?id=0B9sPiD5EdfD-dU1xb1JrbUtNUlE)

- Trying to use function CORREL on Excel for possible connection between different data. Some pairs show strong connections such as different elements concentration in air, or the humidity and temperature.

Where are these data from? 

 - 13 schools in [Greece](https://drive.google.com/open?id=1MP6JIzob6P2g3Kvq-l-yRYSZAXE&usp=sharing)

### setup enviroment : Cassandra and Flink on macOS Sierra 10.12+

`brew install python` (may or maybe not need to use it but I run this anyway)

`pip install cql --ignore-installed six`

   --ignore-installed six is due to [fail of updating six on macOS 10.11](https://github.com/donnemartin/haxor-news/issues/54)
    
`brew install cassandra`
    
`brew install apache-flink`

__Why use Cassandra, Flink__

1. __Flink__

    Flink is well-designed on processing [streaming data](https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/datastream_api.html) 
    comparing with Spark, Hadoop.
    
    That is the crucial requisition for processing the sensor data
    
    Flink also has [connector for Cassandra as data sink](https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/connectors/index.html)

2. __Cassandra__

    We are dealing with sensors data, which means a lot of machines, continuous emission and a large amount of data
    
    Maybe we can keep using Cassandra which we can find a tons of [lectures how to use it properly](https://academy.datastax.com/courses)
    
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
    
    __Challenge__:
    - Unlike in the SQL world where you model your data first and then write the queries, in Cassandra you need to figure out all the queries that will be done and model your data accordingly.
    It means you need to think twice when [building the data model](https://medium.com/@jscarp)


(**_TODO_** More details will be added here)
    
## Retrieve the data

- [x] [Retrieve the data](https://github.com/nanazhu/Thesis/tree/master/RetrieveData) by using APIs on  https://api.sparkworks.net/swagger-ui.html
  
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
  
 (**_TODO_** More details will be added here)

## Store the data for later processing 
## Process the data


###Ref
1. [The building data genome project](https://github.com/buds-lab/the-building-data-genome-project)
2. [Big data computing](https://piazza.com/uniroma1.it/spring2017/1044406/resources) 
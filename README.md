# Thesis
    This will be a story about "data"

#### Known Issues:
- when we request data within the time range, for different granularity , 
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
[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0] while the real data is not all-zeros in https://console.sparkworks.net/resource/view/90946
    
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


### setup enviroment : Cassandra? and Flink on macOS Sierra 10.12+

`brew install python` (may or maybe not need to use it but I run this anyway)

`pip install cql --ignore-installed six`

_--ignore-installed six is due to [fail of updating six on macOS 10.11](https://github.com/donnemartin/haxor-news/issues/54)_
    
`brew install cassandra`
    
`brew install apache-flink`

__Why use Cassandra, Flink, and  ?__

1. Quote from a wise man(a.k.a. Tyrion Lannister): It is almost a relief to confront a comfortable, familiar monster like ~~my sister~~ Flink in [Big data computing](https://piazza.com/uniroma1.it/spring2017/1044406/resources) 

2. The processor of the data

Flink is good on [streaming data](https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/datastream_api.html) and also has [connector for Cassandra as sink](https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/connectors/index.html)

3. The sink of the data 

Cassandra is good on scalability , mapreduce support , distributed(since we have several data source distributed in different countries in the whole ~~Westeros~~ Europe) and [support JSON](https://www.datastax.com/dev/blog/whats-new-in-cassandra-2-2-json-support). Pre-study [Big data management](https://www.slideshare.net/ZHUNa1/cassandra-tutorial-76288142) 


4. What would be the source for flink input data? 

Maybe we can keep using Cassandra as mentioned in [DS320: DataStax Enterprise Analytics with Apache Spark](https://academy.datastax.com/resources/getting-started-apache-spark)

5. How is the format for the real time input data? 

(**_TODO_** More details will be added here)
    
## Retrieve the data

- [x] [Retrieve the data](https://github.com/nanazhu/Thesis/tree/master/RetrieveData) by using APIs on  https://api.sparkworks.net/swagger-ui.html 
 
 (**_TODO_** More details will be added here)

## Store the data for later processing 
## Process the data

# Thesis
    This will be a story about "data"
   #### Known Issues:
- when we request data within the time range, for different granularity , 
 the response time stamps are different 
     - 5min : it's the code running time, not the fixed data timestamp as below(__tricky part__ !!!! need more time to working on)
     - 1hour : beginning of every hour 
     - 1day : 21:00 for each day 
     - 1month :  at 21:00 last day of the month

- APIs options from Apache Flink 
    - MongoDB (customized need to try the functions?)
    - MariaDB (API for SQL write and read?) 
    - Hadoop (API for both write and read) 
    - Cassandra (API for write)

### setup enviroment : Cassandra? and Flink on macOS Sierra 10.12+

`brew install python` (may or maybe not need to use it but I run this anyway)

`pip install cql --ignore-installed six`

_--ignore-installed six is due to [fail of updating six on macOS 10.11](https://github.com/donnemartin/haxor-news/issues/54)_
    
`brew install cassandra`
    
`brew install apache-flink`

__Why use Cassandra?, Flink, and xxx ?__

1. Quote from a wise man(a.k.a. Tyrion Lannister): It is almost a relief to confront a comfortable, familiar monster like ~~my sister~~ Flink in [Big data computing](https://piazza.com/uniroma1.it/spring2017/1044406/resources) 

2. The processor of the data

Flink is good on [streaming data](https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/datastream_api.html) and also has [connector for Cassandra as sink](https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/connectors/index.html)

3. The sink of the data 

Cassandra is good on scalability , mapreduce support , distributed(since we have several data source distributed in different countries in the whole ~~Westeros~~ Europe) and [support JSON](https://www.datastax.com/dev/blog/whats-new-in-cassandra-2-2-json-support). Pre-study [Big data management](https://www.slideshare.net/ZHUNa1/cassandra-tutorial-76288142) 


4. What would be the source for flink input data? 

5. How is the format for the real time input data? 

(**_TODO_** More details will be added here)
    
## Retrive the data

- [x] [Retrive the data](https://github.com/nanazhu/Thesis/tree/master/RetriveData) by using APIs on  https://api.sparkworks.net/swagger-ui.html 
 
 (**_TODO_** More details will be added here)

## Store the data for later processing 
## Process the data
## Patten behind the data

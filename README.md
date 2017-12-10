
# Mining Sensor Data to Evaluate Indoor Environmental Quality of Public Educational Buildings

In this project, we will collect, extract, transform, load, and analyse sensor data transmitted 
from large amount of sensors installed in school buildings across three European countries. 
The sensor data report multiple types of information including temperature, humidity, outdoor weather, 
electronic consumption, human activities, and etc. 

Using python library such as Pandas, Matplotlib, Numpy and Comfort Tools from Berkeley , 
we aim to predict the overall comfort and other KPI for indoor environmental quality in the school buildings 

Based on our findings, school management can optimise the energy consumption.

The ultimate target :  __Save the energy Save the world__   
    
## Basic information
### Why do we care about collecting data in schools? 
From six until we are ready to step out to get to work, most of our time spent at school, 
sometimes even after school we still stay there for sports activities, or stay in the library for studying. 

What should be an ideally school? 
Enough illumination, comfortable temperature not too hot not too cold, 
enough fresh air to cool you head down before you crashed down by study and/or stress?

The governments spend a lot of money on education, 
but is that enough money or is there some way to put the money on the better place instead of just paying the electricity bills?  

If we really want to do something for making a better place to study, 
where should we start to do or look at? 
We need some first-hand data to tell us which part is the most expensive one so we can start to work on it. 

In this situation, sensor data is good cutting point for having an observation and do the analysis. 
 

### Sensor device geography distribution 

 - Locations in [Google Map](https://drive.google.com/open?id=1MP6JIzob6P2g3Kvq-l-yRYSZAXE&usp=sharing)

![map](./image/map.png?raw=true"")

| Country  | Parameter | Number | Comment | 
| :------: | :------: | :------: | :------: |
| Greece  | Sensing endpoints | 872 | Each sensor equals 1 sensing endpoint | 
| | Sensing rate | 1 minute | Can be modified | 
| | Educators | 294 | Greek public schools in GAIA| 
| |Students | 2267 | Greek public schools in GAIA | 
| Italy (Roma) | Sensing endpoints  | 118 |Will soon be augmented|
| | Sensing rate | 1 minute | Can be modified | 
| | Educators | 120 | University faculty and Post Doc |
| | Students | 1706 | University students | 
|Italy(Prato)|Sensing endpoints |117 |
| Sweden(Soderhamn)|Sensing endpoints |3 | create on 2017-07-12T12 more to add, no data yet| 

Total : 16 sites and 1922 sensors are on the record till 2017/09/16.
Part of them are newly installed in this year and some have history data from 2015.
The data is collected under different weather condition, from different cultures , with different user behaviour pattens 
It is a good start we could see the difference and find the similarity.

### Sensor data : unit  
- Power consumption
    - Calculated Power Consumption : mWh
    - Power Consumption : mWh
    - Electrical Current : mA/A
    - Active Power : mW
    - Apparent Energy : Vah
    - Apparent Power : VA
    - Voltage : V
    
    - Power Factor : Raw Value
    - Reactive Energy : VARh
    - Reactive Power : VAR
    
- Environmental parameters
    - Noise : Raw Value
    - Motion : Raw Value
    - Movement : Raw Value
    - Luminosity : Raw Value
    - Light : lux

    - Atmospheric Pressure : kPa
    - External Relative Humidity : %
    - Relative Humidity :  %
    - Rain Height : mm
    
    - Wind Direction : degrees
    - Wind Speed : m/sec
    - Temperature : Centigrade
    - External Temperature : C
    
    - Radiation ：uSv/h ( be careful, too high, you will die ) 
    - External Air Contaminants : Raw Value
    - External Ammonia Concentration : Raw Value
    - External Carbon Dioxide Concentration : Raw Value
    - External Carbon Monoxide Concentration : Raw Value
    - External Oxygen Concentration : Raw Value
    - Carbon Monoxide Concentration : Raw Value
    - Methane Concentration : Raw Value
    
![sensortype](./image/sensorType.png?raw=true "")
 ### Sensor device connection 
- WiFi 


    
## Accessibility
 - This table includes large range of data which are missing due to sensors no longer working or the whole sites are power off  
        during [2015-Nov-1,2017-Oct-30]
                    
    |ID |	Name	| Inactive | start time  | outlier| Total number of measurements | 
    | :------------: | :------------: | :------------:| :------------:|:--------:|:------:| 
    |144024	|Δημοτικό Σχολείο Λυγιάς|	30.48 %| before 2015-10-30| 16.49% | 73,000
    |144242	|1ο Γυμνάσιο Ν. Φιλαδέλφειας	|2.94 %| before 2015-10-30|10.90% |  94,900 |
    |144243	|Δημοτικό Σχολείο Μεγίστης	|24.49 %|before 2015-10-30|15.80% | 64,970|
    |155076	|Gramsci-Keynes School	|4.56 %	|  2016-08-04|39.77%|39.77% |120,450|
    |155077	|Sapienza	|58.22 %	| 2016-10-29|51.01%|177,390|
    |155849	|6ο Δημοτικό Σχολείο Καισαριανής|	22.98 %|	before 2015-10-30|16.76%|52,560
    |155851	|5ο Δημοτικό Σχολείο Νέας Σμύρνης	|29.23 %|2016-08-02|39.25% | 75,190|
    |155865	|46ο Δημοτικό Σχολείο Πατρών	|38.72 %	|2016-09-22|38.28% | 53,290 | 
    |155877	|2ο Δημοτικό Σχολείο Παραλίας Πατρών|35.18 %	|2017-02-01|45.80% | 46,720| 
    |157185	|Ελληνογερμανική Αγωγή	|3.31 %|2017-02-01|40.50% | 123,370
    |159705	|Soderhamn	|0.00 %|2017-09-21|48.24% | 70,810
    |19640	|Γυμνάσιο Πενταβρύσου Καστοριάς	|1.72 %	|before 2015-10-30 | 20.34% | 112,420
    |27827	|8ο Γυμνάσιο Πατρών	|3.71 %	|before 2015-10-30|10.71% | 71,540 | 
    |28843	|2ο ΕΠΑΛ Λάρισας	|43.08 %	|before 2015-10-30|22.17% | 117,530
    |28850	|55o Δημοτικό Σχολείο Αθηνών	|21.23 %|before 2015-10-30|22.36% | 91,250
    
    ![active](./image/active_15sites.png?raw=true"")
    sensor device activities  
    ![active](./image/active15_device.png?raw=true"")
     - In this table, statistic for sensors belong to three different vendors and different ways to  transfer data in the whole sensors

     | Name	|Inactive |   outlier| Total number of measurements | 
     | :------: | :------: | :------:| :------:| 
     | Libelium for outdoor weather  | 15.16% |  10.61% | 31,390     
     | Synfield for outdoor weather | 14.40 %| 9.28%|10,950
     | Electrical Power Consumption |18.68 % |  33.40 %| 73010
 ![active](./image/active_3types.png?raw=true"")

## Reliability
__ETL for raw data__
- Clean the __times period__ which all the sensors are __inactive__.
- Clean the __sensors__ which are always __inactive__  

     - If the site is not kick-off yet, it will not be counted as inactive
     - Only after sensor(s)(maybe just a few of them)actived, start to count inactive missing data
     - In order to present all data in the one heatmap, here use [Feature scaling](https://en.wikipedia.org/wiki/Feature_scaling) to normalize the values into [0,1]   

- Remove the outliers with Turkey's fences and replace with min/max value
    ```
    What is outliers 
        In statistics, an outlier is an observation point that is distant from other observations.
        An outlier may be due to variability in the measurement or it may indicate experimental error; 
        the latter are sometimes excluded from the data set.[3] Outliers can occur by chance in any distribution, 
        but they often indicate either measurement error or that the population has a heavy-tailed distribution. 
        In the former case one wishes to discard them or use statistics that are robust to outliers, 
        while in the latter case they indicate that the distribution has high skewness 
        and that one should be very cautious in using tools or intuitions that assume a normal distribution. 
        A frequent cause of outliers is a mixture of two distributions, which may be two distinct sub-populations, 
        or may indicate 'correct trial' versus 'measurement error'; this is modeled by a mixture model.
    
        Output Two ways to indicate a data point is an outlier
         - Real-valued outlier score, higher values of the score make the point more like an outlier
         - Binary label binary value yes or no for an data point to be outlier
     ``` 
- identify outliers by using Turkey's fences, aka __inter quartile range__ 
        
    `Q1 = First Quartile`
    
    `Q3 = Third Quartile`
    
    `Inter-quartile Range (IQR) = Q3 - Q1`
    
    `Lower Outlier Boundary = Q1 - 3 * IQR`
    
    `Upper Outlier Boundary = Q3 + 3 * IQR`

- identify outliers by using a sliding windows W holds last W-1 values
    Moving windows through data from the beginning
    - If the __inter quartile range__ becomes biggest ever seen,here comes a outliers : replace it with min or max 
    - If the new value is NaN, it is also an outlier : replace it with average 
    - min/max/average = min/max/average (previous W-1 values)
    
- moving window average to smooth out short-term fluctuations and highlight longer-term trends or cycles
    ```
    The SMA is the most straightforward calculation, the average over a chosen time period. 
    The main advantage of the SMA is that it offers a smoothed line, less prone to whipsawing up and down in response to slight, 
    temporary price swings back and forth. Therefore, it provides a more stable level indicating support or resistance. 
    The SMA's weakness is that it is slower to respond to rapid changes that often occur at market reversal points. 
    The SMA is often favored by analysts operating on longer time frames, such as daily or weekly charts.
    ``` 
- refill the NaN with average of the whole series values
 ![ETL](./image/ETL_demo1.png?raw=true"")
- Linear fit 
    ```Linear regression
    In statistics, linear regression is a linear approach for modeling the relationship 
    between a scalar dependent variable y and one or more explanatory variables 
    (or independent variables) denoted X.```
- Visualize one day temperature data after processes mentioned above
![trend](./image/ELT.png?raw=true"")


## Accuracy
Can we retrieve outdoor weather through API ? 
   
[Openweathermap for real-time data](https://openweathermap.org/current#current_JSON)

But this response is only for the real time request.

[Worldweatheronline for history data](https://developer.worldweatheronline.com/api/docs/historical-weather-api.aspx#hourly_element)

Both of APIs response : 

| Temperature | Wind | Humidity | Pressure | Cloud...|
| ----------  | --------| ----------| ----------|----------|

What about the accuracy between data retrieved from API and sensors?
![hist](./image/APIvsSensor.png?raw=true"") 
![hist](./image/144243External%20Relative%20HumidityAPI.png?raw=true"")
__Notice__ API from worldweatheronline does not provide longer than 32days data
 
Conclusion : Yes we can retrieve both realtime and history,but the accuracy is not pretty enough


## Interpretation
### Orientation Prediction and Temperature Deviation 

- Assuming the indoor temperature should rise as the day time passing by. 
  We do not put human activity or others into the consideration, for now
  
- __Identify patten by peak time:__
    - Intuitively while observing the temperature peak for different rooms: 
        - East: the peak temperature mostly arrives at the early day
        - West: the peak  at the late of the day
        - South: the peak should be at the mid-noon or later 
        - Rest: room facing north/music room/computer lab/basement room will have relatively low variation and average of the temperature
    ![peak](./image/peakAfterETL.png?raw=true"")
    ![peak](./image/peakETL.png?raw=true"")
    ![peak](./image/peakETL_one_day.png?raw=true"")
    If we put cloud cover persentage with orientation of the room.
    Observe the time difference for indoor vs outdoor reach daily peak temperature 
    ![peak](./image/diff_cloud.png?raw=true"")
    
    - Predicting orientation by using peak temperature :
        - Using RESTful API to retrieve the time : [sunrise , noon ,and sunset],unit: hour.
        - Only check the temperature during the daytime between [sunrise,sunset]
        - pick the hottest time and put that time:hour into [list_peak_at_hour] 
        - __Orientation = sum( [list_peak_at_hour]  /24hour *360degree)/length([list_peak_at_hour] )__ _(unit:degree)_   
        - Simply match Orientation into :
            - 0-90 degree: North-East
            - 90-180 degree: South-East
            - 180-270 degree: South-West
            - 270-360 degree: North-West
            
            (./image/comp1.jpg)
       
        ``` 
        Exmaple:8ο Γυμνάσιο Πατρών, 
        Class 1 id fb8, the classroom reach the highest temperature at hour: 
        peak_at_hour_list = [ 18, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 18, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17,
        17, 17, 17, 16, 16, 16, 16, 18, 17, 16, 16, 17, 16, 17, 16, 16, 17, 16, 18, 17, 17, 17, 16, 16, 17, 17, 16, 17,17, 
        16, 16, 17, 17, 17, 16, 17, 17, 18, 17, 17, 16, 16, 17, 17, 17, 17,17, 17, 16, 17, 17, 16, 17, 15, 16, 17, 16, 16, 
        16, 16, 16, 16, 17, 17, 16, 17, 16, 17, 17, 15, 16, 17, 17, 17, 16, 16, 16] 
        in total: 
        [(16 o'clock, 33times), (18 o'clock, 5times),(15 o'clock, 2times), (17 o'clock, 60times)]
        Orientation = sum(16/24*360*26+...+15/24*360*2+17/24*360*60)/Length(peak_at_hour_list) = 250.2 degree
        So we got the orientation is 250.2 degrees which looks like south-west. 
        ```
        ![peak ](./image/27827_peak_perday.png?raw=true"") 
        ```
        But ... if there are also a lot of peaks happened in the morning  
        ClasslB2 id :0x317.
        This room gets enough exposed in the sunshine/high temperature during the morning and also likely it facing to the south-east
        in total: (14 o'clock, 5times), (16 o'clock, 3times), (10 o'clock, 3times), (12 o'clock, 3times), 
        (11 o'clock, 3times), (15 o'clock, 3times),(13 o'clock, 2times), (18 o'clock, 2times), (17 o'clock, 2times), 
        (7 o'clock, 1times), (19 o'clock, 1times)
        Orientation = 205.7 degree,have peak temperature in the mornings at [7, 10, 11], takes 25.0% in total
        we got something more like heading to the east , or south east
        ```
        ![peak ](./image/27827classB2_peak.png?raw=true"") 
    We take a close look at the distribution of site rooms in  different orientation 
    The south-east are in lower temperature compared with south and south-west room  
    ![hist](./image/Figure7.png?raw=true"")

         Category by site :
     
        |SITE ID| Orientation Prediction Correct |
        | :----:| :---------:  |
        |144024| 60%
        |144243	| 50%|
        |155851	| 20%|
        |155865	|50%	|
        |19640| 0% | 
        |27827	|25%|
        |144242	| 0%|
        |155877	|33%|
        |159705| 28% | 
        |155849 | 0%|
        |157185| 44% | 
        | REST (no data) |   
    
       Category by the room orientation : 
    
        |Orientation | Orientation Prediction Correct | comment 
        | :----:| :---------:  |:---------:  |
        |N - E| 0% | 0/12 |
        |E - S| 35% | 6/17 | 
        |S - W| 56% | 9/16| 
        | W  | 0%| 0/1| 
        | W - N|0% |  0/11|
    
    - The cloudy coverage impact on the indoor temperature:
        ![cloudy](./image/CloudyFeb-Oct144242.png?raw=true"") 
        ![cloudy](./image/CloudyFeb-Oct155877.png?raw=true"")
        ![cloudy](./image/CloudyFeb-Oct155865.png?raw=true"")
        ![cloudy](./image/CloudyFeb-Oct155851.png?raw=true"")

- **TODO** __Identify patten by slope = delta(temperature)/delta(time):__ 
    
    - slope for one room,first picture is the data after ETL, second is the slop for the data
    ![trend](./image/slope.png?raw=true"")
    But we can't see pattern so obviously
    - if it is not easy to see the patten,then put several subsites in one day together
    
        raw data
    ![](./image/27827_raw.png?raw=true"")
    slope of the data
    ![](./image/27827_diff.png?raw=true"")
    heatmap for the slop of the data
    ![](./image/27827_diff_heatmap.png?raw=true"")

### Comfort

From Wikipedia
        
 > __Thermal comfort__

    Thermal comfort is the condition of mind that expresses satisfaction with the thermal environment 
    and is assessed by subjective evaluation (ANSI/ASHRAE Standard 55).
        
 > __ANSI/ASHRAE Standard 55__
        
    (Thermal Environmental Conditions for Human Occupancy) is a standard 
    that provides minimum requirements for acceptable thermal indoor environments. 
    The purpose of the standard is to specify the combinations of indoor thermal environmental factors 
    and personal factors that will produce thermal environmental conditions 
    acceptable to a majority of the occupants within the space
    
    The standard addresses the four primary environmental factors 
    (temperature, thermal radiation, humidity, and air speed) 
    and two personal factors (activity and clothing) that affect thermal comfort. 
    It is applicable for healthy adults at atmospheric pressures in altitudes up to (or equivalent to) 3,000 m (9,800 ft), 
    and for indoor spaces designed for occupancy of at least 15 minutes.

 > Comfort zone 
 
    Refers to the combinations of air temperature, mean radiant temperature (tr), 
    and humidity that are predicted to be an acceptable thermal environment at particular values of 
    air speed, metabolic rate, and clothing insulation (Icl)

Intuitively, we want the temperature indoor in the certain range like [18,24]
during Monday to Friday, from 8:00 to 18:00
Obviously, the truth is not always what we wish for 
![hist](./image/27827_hist.png?raw=true"")


Tool: [CBE Thermal Comfort Tool for ASHRAE-55 ](http://comfort.cbe.berkeley.edu)
    How to use:
    
    By choosing the Adaptive method at the very top of the user interface, 
    the chart changes and the input variables include air temperature, mean radiant temperature and prevailing mean outdoor temperature. 
    This is because the personal factors and humidity are not significant in this method since adaptation is considered, and the only variable is the outdoor temperature.
    See above for explanation of the first two variables, air and mean radiant temperature.
    
    Prevailing mean outdoor temperature
    Here you can type the outdoor temperature averaged as explained on the standard. 
    See the Wikipedia link for a brief explanation.
    Changing this variable makes the dot representing the current condition move horizontally. 
    The meaning of this chart is that certain conditions of indoor-outdoor temperature fall inside the comfort zone, 
    which in this case is static.

![hist](./image/144243Comfort.png?raw=true"")

- Sample period 2017.Sep.05-2017.Nov.04 , weekday: Monday-Friday, Time: 8:00-16:00, week of year: 36-44
- In daytime, this room is comforable for (n*8hours) , 0 < n < 1
- All day comfortable = 1 all day not comfortable = 0

    - The following two pictures are comfortness ratio per day is based on hourly temperature from site ID 27827 8ο Γυμνάσιο Πατρών
        - use "Site-Temperature" sensor as outdoor temperature source 
![heatmap](./image/27827_comfortable.png?raw=true"")
        - Use Worldweatheronline API as outdoor temperature source 
![heatmap](./image/27827_comfortableAPI.png?raw=true"")
        - The difference might because of the not accuracy from "Site-Temperature"

    - The following two pictures are comfortness ratio per day is based on hourly temperature from site ID 144242,1ο Γυμνάσιο Ν. Φιλαδέλφειας
        - use libelium sensor as outdoor temperature source 
![heatmap](./image/144242_comfortable.png?raw=true"")
        - Use Worldweatheronline API as outdoor temperature.
![heatmap](./image/144242_comfortableAPI.png?raw=true"")
        - The difference might because of the "complete nonsense data" from [this libelium sensor](https://console.sparkworks.net/resource/view/145588)
    
    - The following two pictures are comfortness ratio per day is based on hourly temperature from site ID 19640,Γυμνάσιο Πενταβρύσου Καστοριάς
            - use libelium sensor as outdoor temperature source 
![heatmap](./image/19640_comfortable.png?raw=true"")
        - Use Worldweatheronline API as outdoor temperature.
![heatmap](./image/19640_comfortableAPI.png?raw=true"")
    
    Since we do not have good stable outdoor temperature resource,API could be an idealy option.   
    ![heatmap](./image/comfort_compare.png?raw=true"")
    

## Data Variability and Potential Patterns 
- Temperature  
- Light
- Motion 
- Power consumption

We take a close look on the raw data and see what kind of story or intuition we might get:

- Demo on all the schools in Greece, for one year, time interval: day
    - Power Consumption, 10 schools in Greece 
    ![greece one year](./image/power_greece_16Sep_17_Sep_perday.png?raw=true "")
    There are 3 other schools without power consumption sensors or no data.
    
    - Temperature, 12 schools in Greece
    ![greece one year](./image/Greece%20Temperature.png?raw=true "")
        
##### Demo on site __8ο Γυμνάσιο Πατρών,Greece__ ,for 3 weeks, time interval: hour        

- Temperature for 4 weeks in the main building with building floor plan 

   So here are the pattens  
   - the room(4ce) at ground floor , heading to the north has lowest temperature all the time.
   - the two on the first floor, two classrooms to west (class 1 and 2) are next to each other and have similar pattern of temperature changing
   and the "warmest" rooms in the whole school building. 
   - the rooms at the north are cooler compared to.
  

![4 weeks temperature](./image/27827%20Temperature1.png?raw=true"")
    
- Clustering luminosity for 4 weeks in the main building with building floor plan 
![4 weeks temperature](./image/27827%20Lumin_vs_temp_cluste1.png?raw=true"")

- Clustering Occupancy(motions vs site-noise) for 4 weeks in the main building with building floor plan 
![4 weeks temperature](./image/27827%20Motion.png?raw=true"")
 
- Temperature 
![3 weeks temperature](./image/8ο%20Γυμνάσιο%20Πατρών%20Temperature.png?raw=true "")
    
- Calculated Power Consumption 
![3 weeks Calculated Power Consumption](./image/8ο%20Γυμνάσιο%20Πατρών%20Calculated%20Power%20Consumption.png?raw=true "")

- Main building Motion 
![3 weeks motion](./image/8ο%20Γυμνάσιο%20Πατρών%20Motion.png?raw=true"")
 
- Subsite Motion (Αίθουσα ισόγειο) 
![3 weeks motion ](./image/Mo2.png?raw=true"")

- Luminosity 
![4 weeks temperature](./image/27827%20Lumin.png?raw=true"")

##### Demo on site __Γυμνάσιο Πενταβρύσου Καστοριάς,Greece__, for 4 weeks, time interval: hour  
- Temperature at the main building with building floor plan
Patterns :
Temperature in Computer Lab is more stable than the rest of others , but still fit our expectation.
![4 weeks Temperature](./image/19640%20Temperature0.png?raw=true"")    
    
- Humidity at the main building with building floor plan
Patterns : we can see the basement has the highest humidity and the music class is stable and remain in a good dry condition for preserving the music instruments 
![4 weeks Temperature](./image/19640%20humidity0.png?raw=true"")
    
    - Clustering the room by temperature at the main building with building floor plan
    ![4 weeks Temperature](./image/19640%20Temperature0_cluster.png?raw=true"")    
    - Clustering the Occupancy(motion vs site-noise)at the main building with building floor plan
    ![4 weeks motion](./image/19640%20Motion.png?raw=true"")
    - Temperature inside & outside of the building
    ![4 weeks Temperature](./image/19640%20Temperature3.png?raw=true"")    
    - Temperature at the sub-site building
    ![4 weeks Temperature](./image/19640%20Temperature2.png?raw=true"")
    - Calculated Power Consumption at the main building 
    ![4 weeks Calculated Power Consumption](./image/19640%20Calculated%20Power%20Consumption.png?raw=true"")  
    - Electrical Current at the main building 
    ![4 weeks ElectricalCurrent](./image/19640%20ElectricalCurrent.png?raw=true"")
    - Luminosity at the main building 
    ![4 weeks Luminosity](./image/19640%20Luminosity1.png?raw=true"")
    - Luminosity at the sub-site building
    ![4 weeks](./image/19640%20Luminosity2.png?raw=true"")
    - Rain Height and Relative Humidity at the main building
    ![4 weeks Rain Height and Relative Humidity](./image/19640%20RainHeight_humidity.png?raw=true"")
    - Relative Humidity at the sub-site building
    ![4 weeks Relative Humidity](./image/19640%20humidity.png?raw=true"")
     - Motion at the main building 
    ![4 weeks motion](./image/19640%20motion1.png?raw=true"")
    - Motion at the sub-site building
    ![4 weeks motion](./image/19640%20motion2.png?raw=true"")
    - Noise at the main building 
    ![4 weeks motion](./image/19640%20noise1.png?raw=true"")
    - Noise at the sub-site building
    ![4 weeks motion](./image/19640%20noise2.png?raw=true"")
    - Motion and Noise at the sub-site building
    ![4 weeks motion](./image/19640%20subsite%20motion%20noise.png?raw=true"")

- Demo on site __Δημοτικό Σχολείο Μεγίστης,Greece__, for 4 weeks, time interval: hour  
    - Temperature inside and outside of the main building with building floor plan
    ![4 weeks Temperature](./image/144243Temperature.png?raw=true"")
    It is not so obvious for the orientation
    - Clustering the temperature inside and outside of the main building with building floor plan
    ![4 weeks Temperature](./image/144243Temperature_cluster.png?raw=true"")
     Now, it is easy to see the orientation



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
    It means you need to think twice when [building the data model](https://medium.com/@jscarp). Now we just use a very simple table as below for current phase
    
    GAIA has 1841 resources including duplicated 538 data sets categorized again in subsites 
    Information for resources under each site :
    
    | Site ID  | ResourceID | Subsite ID | URI | Property
    | :------:  | :------:| :------: | :------:| :------: |
    | int |  int |int | text | text | 
    
    Information for sites :
    
     | Site(SubSite) ID  | Latitude | Longtitude | Name |
     | :------:  | :------:| :------: | :------: | 
     | int | float | float | text | 
    
    API data for weather
    
    | Site ID  | Timestamp |Type| value |  
    | :------:  | :------:| :------: | :------:| 
        
    The data collected every 5mins from the resources: [Version 1 ]
    
    | Resource ID | Timestamp |reading |
    | :------:| :------: | :------: |
    | int | timestamp | float | 
    
    Design for the above table which stores time series data
    ![pic](./image/cassandra_table_by_date.png?raw=true "")
    
    Cassandra tutorials : [Basic Time Series Data](https://academy.datastax.com/resources/getting-started-time-series-data-modeling) 
    and [Advanced Time Series](https://www.datastax.com/dev/blog/advanced-time-series-with-cassandra)
    - The Primary Key is equivalent to the Partition Key in a single-field-key table.
    - The Composite/Compound Key is just a multiple-columns key: 
        - The first part of the key is called PARTITION KEY
        - The second part(the rest after first part) of the key is the CLUSTERING KEY 
    - The Partition Key is responsible for data distribution across your nodes.
    - The Clustering Key is responsible for data sorting within the partition.
    
    The data collected every 5mins from the resources: [Version 2]

    |  Resource ID | Date | Timestamp |value |
    | :------:| :------: |:------: | :------: |
    | int | text | timestamp | float | 
        
    ```
    CREATE TABLE gaia.read_data (
    id int,
    timeindex timestamp,
    value float,
    date text,
    PRIMARY KEY ((id,date), timeindex )
    ) WITH CLUSTERING ORDER BY (timeindex ASC);
    ```   
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
                ....
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
    

## Supervisors :
  Professor Ioannis Chatzigiannakis   
  Professor Aris Anagnostopoulos
## Reference
1. [The building data genome project](https://github.com/buds-lab/the-building-data-genome-project)
2. [Big data computing](https://piazza.com/uniroma1.it/spring2017/1044406/resources) 
3. [Naked Statistics: Stripping the Dread from the Data by Charles Wheelan](https://www.goodreads.com/book/show/17986418-naked-statistics)
4. Bird view for the GAIA project : 

__GAIA Architecture design overview__
 ![archetecture](./image/GAIAarchitecturedesignOverview.jpg?raw=true "")

__Cloud platform__
 ![cloud](./image/GAIAcloudplatform.png?raw=true "")


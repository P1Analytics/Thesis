import requests
import json
import csv
import pandas as pd
from datetime import datetime, date
def coordinate_dicts():
    global coordinates
    with open("coordinates.txt") as f:
        lines = f.read().splitlines()
    coordinates = {}
    for line in lines:
        coordinates[line.split()[0]] = [line.split()[1], line.split()[2]]
    return coordinates

if __name__ == "__main__":

    coordinate_dict = coordinate_dicts()
    site_list = [144242, 27827, 144024,155076, 155849, 155077, 155865, 155877, 28843, 144243, 28850, 159705, 157185, 155851, 19640]
    
    df_peak_API_Cloud = pd.DataFrame(columns=site_list)
    df_peak_API_TempC= pd.DataFrame( columns=site_list)
    df_peak_API_Humidity = pd.DataFrame(columns=site_list)

    for site_i in site_list:
        lng, lat = coordinate_dict[str(site_i)][0], coordinates[str(site_i)][1]
        coordinate = str(lat)+","+str(lng)
        URL = "http://api.worldweatheronline.com/premium/v1/past-weather.ashx"
        
        date_range=[
                    '1-1','1-31',
                    '2-1','2-28',
                    '3-1,','3-31',
                    '4-1','4-30',
                    '5-1','5-31',
                    '6-1','6-30',
                    '7-1','7-31',
                    '8-1','8-31,',
                    '9-01','9-30',
                    '10-1','10-31',
                    '11-1','11-30',
                    '12-1','12-31',
                    ]
        Y='2015'
        Year = +"-"

        result_listTempC = []
        result_listHumidity = []
        result_listCloud = []

        while date_range:
            begin = Year+date_range[0]
            end = Year+date_range[1]
            params = {
                'q':coordinate,
                'date': begin,
                'enddate': end,
                'tp': '1', #'1',
                'key': '612818efa9204368a1785431172610',
                'format': 'json',
                'includelocation': 'yes',
            }
            date_range.pop(0)
            date_range.pop(0)

            r = requests.get(URL, params).json()
            print(json.dumps(r, sort_keys=True, indent=4)) # human-readable response :)
            for i in r["data"]["weather"]:
                for j in i["hourly"]:
                    result_listTempC.append(int(j["tempC"]))
                    result_listCloud.append(int(j["cloudcover"]))
                    result_listHumidity.append(int(j["humidity"]))
        print(df_peak_API_Cloud.shape,len(result_listCloud))
        df_peak_API_Cloud[site_i]=result_listCloud
        df_peak_API_Humidity[site_i]=result_listHumidity
        df_peak_API_TempC[site_i]=result_listTempC

    df_peak_API_TempC.to_csv("API_tempC_"+ Y  +".csv", sep=";")
    df_peak_API_Humidity.to_csv("API_Humidity_" + Y+ ".csv", sep=";")
    df_peak_API_Cloud.to_csv("API_Cloud_" + Y + ".csv", sep=";")
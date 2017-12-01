import requests
import json
from db_util import *


if __name__ == "__main__":

    with create_connection('/Users/nanazhu/Documents/Sapienza/Thesis/src_python/test.db') as conn:
        try:
            c = conn.cursor()
            coordinate_dict = query_site_lat_lng(c)
        except Error as e:
            print("SQL ERROR:", e)

    site_list = [144242, 27827, 144024,155076, 155849, 155077, 155865, 155877, 28843, 144243, 28850, 159705, 157185, 155851, 19640]
    
    df_peak_API_Cloud = pd.DataFrame(columns=site_list)
    df_peak_API_TempC= pd.DataFrame( columns=site_list)
    df_peak_API_Humidity = pd.DataFrame(columns=site_list)

    for site_i in site_list:
        lng, lat = coordinate_dict[site_i][0], coordinate_dict[site_i][1]
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
        Y='2016'
        Year = Y+"-"

        result_listTempC = []
        result_listHumidity = []
        result_listCloud = []
        result_listTime = []
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
                date = i["date"]
                for j in i["hourly"]:
                    result_listTempC.append(int(j["tempC"]))
                    result_listCloud.append(int(j["cloudcover"]))
                    result_listHumidity.append(int(j["humidity"]))
                    result_listTime.append(str(date)+" "+ '{:02d}'.format(int(int(j["time"])/100))+":00"+":00")

        df_peak_API_Cloud[site_i]=result_listCloud
        df_peak_API_Humidity[site_i]=result_listHumidity
        df_peak_API_TempC[site_i]=result_listTempC

    df_peak_API_Cloud["timestamps"] = result_listTime
    df_peak_API_Cloud = df_peak_API_Cloud.reset_index(drop=True)
    df_peak_API_Cloud = df_peak_API_Cloud.set_index('timestamps')
    df_peak_API_Cloud.to_csv("API_Cloud_" + Y + ".csv",sep=";")

    df_peak_API_TempC["timestamps"] = result_listTime
    df_peak_API_TempC = df_peak_API_TempC.reset_index(drop=True)
    df_peak_API_TempC = df_peak_API_TempC.set_index('timestamps')
    df_peak_API_TempC.to_csv("API_tempC_"+ Y  +".csv", sep=";")

    df_peak_API_Humidity["timestamps"] = result_listTime
    df_peak_API_Humidity = df_peak_API_Humidity.reset_index(drop=True)
    df_peak_API_Humidity = df_peak_API_Humidity.set_index('timestamps')
    df_peak_API_Humidity.to_csv("API_Humidity_" + Y+ ".csv",sep=";")

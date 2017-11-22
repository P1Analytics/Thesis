import requests
import json
import csv
import pandas as pd

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

    date_list = pd.date_range('2017-9-5', periods=61, freq='D')

    df_peak_API = pd.DataFrame(index=date_list,columns=site_list)

    for site_i in site_list:
        lng, lat = coordinate_dict[str(site_i)][0], coordinates[str(site_i)][1]
        coordinate = str(lat)+","+str(lng)
        URL = "http://api.worldweatheronline.com/premium/v1/past-weather.ashx"

        date_range=['2017-9-5','2017-10-4','2017-10-5','2017-11-4']
        result_list = []
        while date_range:
            params = {
                'q':coordinate,
                'date': date_range[0],
                'enddate': date_range[1],
                'tp': '24',
                'key': '612818efa9204368a1785431172610',
                'format': 'json',
                'includelocation': 'yes',
            }
            date_range.pop(0)
            date_range.pop(0)
            r = requests.get(URL, params).json()
            # print(json.dumps(r, sort_keys=True, indent=4)) # human-readable response :)
            for i in r["data"]["weather"]:
                for j in i["hourly"]:
                    result_list.append(int(j["FeelsLikeC"])) # ["humidity","tempC","cloudcover","FeelsLikeC"]
        df_peak_API[site_i]=result_list

    df_peak_API.to_csv("API_FeelsLikeC.csv", sep=";")
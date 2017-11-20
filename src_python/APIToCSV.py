import requests
import json
import csv

def coordinate_dicts():
    global coordinates
    with open("coordinates.txt") as f:
        lines = f.read().splitlines()
    coordinates = {}
    for line in lines:
        coordinates[line.split()[0]] = [line.split()[1], line.split()[2]]
    # print(coordinates)
    return coordinates


if __name__ == "__main__":


    print("********* replace outdoor with API *************")
    # real-time
    # r = requests.get('http://api.openweathermap.org/data/2.5/weather?',
    #                  params={'lat': lat, 'lon': lng, 'units': 'metric',
    #                          'APPID': 'bd859500535f9871a59b2fa52547516e'}).json()
    # print("the real time query:\n", r)

    # history
    # TODO API KEY expired on Dec25 https://developer.worldweatheronline.com/api/docs/historical-weather-api.aspx
    data_type = "humidity"  # TODO  you can change into any kind of data you want instead of Temperature into API.csv
    date_list = ['2017-9-8','2017-9-21']#,'2017-10-5','2017-11-4'] # begin,end into pairs
    with open('API.csv','w')as f:
        csv_api=csv.writer(f,lineterminator='\n')
        coordinate_dict = coordinate_dicts()
        for site in [144242,27827,144024,155076,155849,155077,155865,155877,28843,144243,28850,159705,157185,155851,19640]:
            site = 19640
            lng, lat = coordinate_dict[str(site)][0],coordinates[str(site)][1]
            coordinate = str(lat)+","+str(lng)


            # print("---------------------------","SITE IS ",site,"---------------------------")
            URL = "http://api.worldweatheronline.com/premium/v1/past-weather.ashx"
            list_weather = [site]

            for i in [0,]:#int(len(date_list)/2)]:
                params = {'q':coordinate,
                          'date': date_list[0+i], 'enddate': date_list[1+i],
                          'key': '612818efa9204368a1785431172610', 'format': 'json',
                          'includelocation': 'yes', 'tp': '1'}
                r = requests.get(URL, params).json()
                print(json.dumps(r, sort_keys=True, indent=4)) # human-readable response :)

                for i in r["data"]["weather"]:
                    for j in i["hourly"]:
                        list_weather.append(int(j[data_type]))
                        # print(i["date"],int(j["tempC"]))
            print(list_weather)
            csv_api.writerow(list_weather)
            break


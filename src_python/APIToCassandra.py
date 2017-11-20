import requests
import json

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

    # API KEY expired on Dec25 https://developer.worldweatheronline.com/api/docs/historical-weather-api.aspx

    data_type = "humidity" # tempC
    date_list = ['2017-9-8','2017-9-21']#,'2017-10-5','2017-11-4'] # begin,end into pairs

    for site in [144242,27827,144024,155076,155849,155077,155865,155877,28843,144243,28850,159705,157185,155851,19640]:
        lng, lat = coordinate_dicts[str(site)][0],coordinates[str(site)][1]
        coordinate = str(lat)+","+str(lng)

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

        # TODO write into cassandra


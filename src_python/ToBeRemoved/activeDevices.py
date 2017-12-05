import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import warnings
from collections import defaultdict
from pandas.tseries.offsets import *
from pylab import *
warnings.filterwarnings(action="ignore", module="scipy", message="^internal gelsd")
pd.options.mode.chained_assignment = None


def outliers_sliding_window(df, window_number):
    window = []
    outlier = 0
    max_range = 0
    for i in range(df.size):  # df is df_col
        item = df[i]
        if len(window) == window_number:
            window.pop(0)
        window.append(item)
        previous = window[:-1]
        if len(previous) == 0:
            continue  # TODO what if the first value is NaN ?

        average = mean(previous)
        Q1 = np.percentile(previous, 25)
        Q3 = np.percentile(previous, 75)
        upper = Q3 + (Q3 - Q1) * 1.5
        lower = Q1 - (Q3 - Q1) * 1.5
        last_min = np.percentile(previous, 0)
        last_max = np.percentile(previous, 100)
        min = np.percentile(window, 0)
        max = np.percentile(window, 100)
        now_range = max - min
        if max_range < now_range:
            max_range = now_range

        if np.isnan(item):  # or item == 0:
            outlier += 1
            window[-1] = average  # last_max
            df[i] = average  # last_max
            # print(item, "change to max", average)

        if len(previous) < window_number - 1:
            continue

        if now_range > max_range * 0.9:  # 0.8:  # unless new peak comes out, OTHERWISE keep calm and carry on
            outlier += 1
            if item < lower:
                # print(item, "change to min", (last_min + min) / 2)
                window[-1] = average  # last_min# (last_min + min) / 2
                df[i] = average  # last_min #(last_min + min) / 2
            if item > upper:
                # print(item, "change to max", (last_max + max) / 2)
                window[-1] = average  # last_max# (last_max + max) / 2
                df[i] = average  # last_max #(last_max + max) / 2

    # print("******************************", df.name, df.size, outlier,outlier_NaN, outlier_NaN / outlier)
    return df, outlier, average


def ETL_outrage(df):
    # NaN , 0 = -1
    # Active = 1

    df[df > 0] = 1

    for head in list(df):
        df_col = df[head]
        if (df_col[df_col == 0]).size + df_col.isnull().sum() == df_col.size:
            df = df.drop(head, 1)
            continue

    if df.shape[1] != 0:
        active_time = []
        df_normT = df.T
        for head in list(df_normT):
            df_col = df_normT[head]
            try:
                if (df_col[df_col == 0]).size + df_col.isnull().sum() == df_col.size:
                    df_normT[head] = df_normT[head].replace(0, -1)
                    df_normT[head] = df_normT[head].replace(np.nan, -1)
                else:
                    active_time.append(head)
            except ValueError:
                continue
        df = df_normT.T
        begin = df.index.get_loc(active_time[0])
    else:
        begin = df.shape[0]

    return df, begin

def ETL_outlier(df, target):
    for head in list(df):
        df_col = df[head]
        if (df_col[df_col == 0]).size + df_col.isnull().sum() == df_col.size:
            df = df.drop(head, 1)
            continue

    if df.shape[1] != 0:
        active_time = []
        dfT = df.T
        for head in list(dfT):
            df_col = dfT[head]
            try:
                if (df_col[df_col == 0]).size + df_col.isnull().sum() == df_col.size:
                    dfT[head] = dfT[head].replace(0, np.nan)
                else:
                    active_time.append(head)
            except ValueError:
                continue
        df = dfT.T
        begin = df.index.get_loc(active_time[0])
    else:
        begin = df.shape[0] - 1

    df_power = df.iloc[begin:]
    df_power[df_power==0]=np.nan

    for head in list(df_power):
        df_col = df_power[head]
        nan = df_col.isnull().sum()
        df_col, outliers, average = outliers_sliding_window(df_col, window_number=8)
        if head == target:
            # print(head,"outlier:",max(outliers - nan,0))
            return max(outliers - nan,0)
    return 0


if __name__ == "__main__":

    # 2015/10/30 - 2017/10/30
    sensor_env = defaultdict(list)  # all the pysical sensors without power/weather/atmos
    sensor_Libelium = defaultdict(list)
    sensor_synfield = defaultdict(list)
    sensor_power = defaultdict(list)

    FullSites_list = [
        "144024_2YEARS.csv",
        "28843_2YEARS.csv",
        "144243_2YEARS.csv",
        "28850_2YEARS.csv",
        "144242_2YEARS.csv",
        "19640_2YEARS.csv",
        "27827_2YEARS.csv",
        "155849_2YEARS.csv",
        "155851_2YEARS.csv",
        "155076_2YEARS.csv",
        "155865_2YEARS.csv",
        "155077_2YEARS.csv",
        "155877_2YEARS.csv",
        "157185_2YEARS.csv",
        "159705_2YEARS.csv",
    ]

    sensor_env[144024].append([
        149078,
        149075,
        149085,
        149088,
        149067,
        149073,
        149064,
        149059,
        205610,
        205609
    ])
    sensor_env[144242].append([
        155924,
        155922,
        155928,
        155931,
        155936,
        155933,
        155937,
        155941,
        155918,
        155920
    ])
    sensor_env[27827].append([
        202128,
        202127,
        202546,
        157477,
        157867,
        157864,
        157422,
        157418,
        157419,
        157426
    ])
    sensor_env[155849].append([
        155157,
        155160,
        155162,
        155163,
        157319,
        157320
    ])
    sensor_env[155865].append([
        # 207291,
        # 205801,
        # 154183,
        # 154171,
        # 206855,
        # 154188,

        205805,
        205801,
        154183,
        155096,
        155087,
        154170,
        155095,
        154188
    ])
    sensor_env[155877].append([
        # 155644,
        # 155640,
        # 155633,
        # 155655,
        # 148065, #synfield-00:06:66:50:6e:c8
        155644,
        155645,
        155636,
        155637,
        155635,
        155631,
    ])
    sensor_env[155077].append([])
    sensor_env[155076].append([
        # 206822,
        # 155466,
        # 155377,
        # 155379,
        # 155390,
        # 155408,
        # 155410,
        # 155406,
        # 155416,
        # 155436,
        # 155367,
        # 155372,
        # 155375,
        # 155462,
        # 155445,
        # 155447,
        # 155450
        155368,
        155367,
        155371,
        155372,
        155374,
        155375,
    ])
    sensor_env[28843].append([
        # 90834,
        # 90825,
        # 90790,
        # 90807,
        # 90814,
        # 90803,
        # 90841,
        # 145584,#145583, #libelium-0006666c129b
        # 148067#148074 #synfield-00:06:66:50:6e:c8
        90836,
        90831,
        90826,
        90822,
        90794,
        90791,
        90807,
        90813,
        90815,
        90818,
        90800,
        90805
    ])
    sensor_env[144243].append([
        # 145170,
        # 147187,
        # 145231,
        # 145176,
        # 145161,
        # 145140,#145135, # libelium-0006666c1c32
        # 146779#146803 # synfield-00:06:66:80:4b:8b
        145167,
        145169,
        145230,
        145231,
        145180,
        145171,
        145168,
        145158
    ])
    sensor_env[28850].append([
        # 90934,
        # 90897,
        # 90922,
        # 90891,
        # 90912,
        # 90928,
        # 147501,
        # # 147657#147653 # synfield-00:06:66:80:61:7f
        90931,
        90934,
        90900,
        90896,
        90916,
        90918,
        90909,
        90914,
        90923,
        90928,
        90902,
        90907
    ])
    sensor_env[159705].append([
        # 154944,
        #                         206214,
        #                         206243,
        #                         206446,
        #                         206456
        206211,
        206212,
        206245,
        206244,
        206447,
        206446,
        206455,
        206456,
        206465,
        206466,
        206460,
        206459,
        206470,
        206471
    ])
    sensor_env[157185].append([
        # 155914,
        #                         155911,
        #                         157490,
        #                         156993,
        #                         156982,
        #                         157027,
        #                         156977,
        #                         157020,
        #                         156974,
        #                         157001,
        #                         156989,
        #                         207300
        156973,
        156972,
        156981,
        156980,
        156983,
        156982,
        156989,
        156988,
        156992,
        156994,
        156997,
        156998,
        157020,
        157019,
        157025,
        157023,
        # 157490 flareEA-1	temp
    ])
    sensor_env[155851].append([
        # 155555,
        # 155567,
        # 156847,
        # 155525,
        # 156843,
        # 155572,
        # # 153775#153777 # Power
        155553,
        155554,
        157189,
        155566,
        156851,
        156849,
        155523,
        155522,
        156845,
        156846,
        155568,
        155571
    ])
    sensor_env[19640].append([
        # 90588,
        # 90544,
        # 90574,
        # 90591,
        # 90572,
        # 90496,
        # 90533,
        # 90580,
        # 90538,
        # 113911,
        # # 145878#144927 # libelium-0006666c1d24
        90601,
        90598,
        90542,
        90541,
        90575,
        90586,
        90583,
        90582,
        90572,
        90568,
        90533,
        90529,
        90580,
        90576,
        90540,
        90536,
    ])

    df_sensor_Env = pd.DataFrame(columns=[])
    sum_NaN = 0
    sum_Valid = 0
    begin_at = set()
    for file_i in FullSites_list:
        df_site_i = pd.read_csv(file_i, delimiter=";", index_col='timestamps', parse_dates=True)

        old_head = list(df_site_i)
        siteID = int(file_i.split("_")[0])
        sensor_devices = sensor_env[siteID][0]
        while sensor_devices:
            id1 = sensor_devices[0]
            sensor_devices.pop(0)
            match1=[head for head in old_head if str(id1) in head]
            id2 = sensor_devices[0]
            sensor_devices.pop(0)
            match2 = [head for head in old_head if str(id2) in head]
            device_x=(match1+match2)
            # print(device_x)
            df_device_x  = df_site_i.loc[:,device_x]
            df_device_ETL,begin = ETL_outrage(df_device_x)
            df_device_ETL[df_device_ETL == 0] = -1
            if begin != df_device_ETL.shape[0]:
                begin_at.add(begin)
                device_x_head = list(df_device_ETL)[0]
                df_sensor_Env[device_x_head]=df_device_ETL[device_x_head]
                # print("add", device_x_head, df_sensor_Env.shape)

        begin = min(begin_at)
        df_poweron =df_sensor_Env.iloc[begin:]
        valid = df_poweron.shape[0]*df_poweron.shape[1]
        outrage = sum(df_poweron[df_poweron == -1].count())
        print(siteID,outrage,valid)
        sum_NaN += outrage
        sum_Valid +=valid
    print("total for env",sum_NaN,"/",sum_Valid,df_sensor_Env.shape)

    # df_poweron = df_device_ETL.iloc[begin:]
    #
    # print(df_poweron[df_poweron == -1].count().values[0],valid)
    # for i in sensor_devices:
    #     matching = [s for s in old_head if str(i) in s]
    #     if len(matching) > 0:
    #         new_head.append(matching[0])
    # if len(new_head) > 0:
    #     df_sensor_devices[new_head] = df_site_i[new_head]
    #
    #     df_poweron = df_site_i[new_head].iloc[begin:]
    #     valid = df_poweron.shape[0]*df_poweron.shape[1]
    #     print(df_poweron[df_poweron == -1].count())
    #     sum_NaN = sum(df_poweron[df_poweron == -1].count())
    #     print(file_i,sum_NaN,valid,df_sensor_devices.shape,len(list(df_site_i[new_head])))

    # new_head = []
    # if len(sensor_Libelium[siteID])>0:
    #     for i in sensor_Libelium[siteID][0]:
    #         matching = [s for s in old_head if str(i) in s]
    #         if len(matching) > 0:
    #             new_head.append(matching[0])
    #     if len(new_head) > 0:
    #         df_sensor_Libelium[new_head] = df_site_i[new_head]
    #
    #         df_poweron = df_site_i[new_head].iloc[begin:]
    #         valid = df_poweron.shape[0]*df_poweron.shape[1]
    #         sum_NaN = sum(df_poweron[df_poweron == -1].count())
    #         print(file_i,"Libelium",sum_NaN,valid,len(list(df_site_i[new_head])))
    #
    # new_head = []
    # if len(sensor_synfield[siteID])>0:
    #     for i in sensor_synfield[siteID][0]:
    #         matching = [s for s in old_head if str(i) in s]
    #         if len(matching) > 0:
    #             new_head.append(matching[0])
    #     if len(new_head) > 0:
    #         df_sensor_Synfield[new_head] = df_site_i[new_head]
    #
    #         df_poweron = df_site_i[new_head].iloc[begin:]
    #         valid = df_poweron.shape[0]*df_poweron.shape[1]
    #         sum_NaN = sum(df_poweron[df_poweron == -1].count())
    #         print(file_i,"synfield",sum_NaN,valid,len(list(df_site_i[new_head])))
    #
    # new_head = []
    # if len(sensor_power[siteID])>0:
    #     for i in sensor_power[siteID][0]:
    #         matching = [s for s in old_head if str(i) in s]
    #         if len(matching) > 0:
    #             new_head.append(matching[0])
    #     if len(new_head) > 0:
    #         df_sensor_Mea[new_head] = df_site_i[new_head]
    #
    #         df_poweron = df_site_i[new_head].iloc[begin:]
    #         valid = df_poweron.shape[0]*df_poweron.shape[1]
    #         sum_NaN = sum(df_poweron[df_poweron == -1].count())
    #         print(file_i,"power",sum_NaN,valid,len(list(df_site_i[new_head])))



    sensor_Libelium[144242].append([145586])
    sensor_Libelium[144024].append([143947])
    sensor_Libelium[19640].append([144927])
    sensor_Libelium[28843].append([145578])
    sensor_Libelium[144243].append([145140])

    SensorLibelium_list = [
        "Libelium_144242.csv",
        "Libelium_144024.csv",
        "Libelium_28843.csv",
        "Libelium_144243.csv",
        "Libelium_19640.csv",
    ]
    sum_NaN = 0
    sum_Valid = 0
    df_sensor_Libelium = pd.DataFrame(columns=[])
    for file_i in SensorLibelium_list:  # FullSites_list:
        df_site_i = pd.read_csv(file_i, delimiter=";", index_col='timestamps', parse_dates=True)
        df_device_X, begin = ETL_outrage(df_site_i)
        siteID = int(file_i.split("_")[1].split(".")[0])
        if len(sensor_Libelium[siteID]) == 0:
            continue
        sensor_device = str(sensor_Libelium[siteID][0][0])
        sensor_device = [head for head in list(df_device_X) if sensor_device in head]
        df_device_X[df_device_X == 0] = -1
        df_poweron = df_device_X[sensor_device].iloc[begin:]
        print(siteID, sum(df_poweron[df_poweron == -1].count()), df_poweron.shape[0])
        sum_Valid += df_poweron.shape[0]
        sum_NaN += sum(df_poweron[df_poweron == -1].count())
        df_sensor_Libelium[sensor_device] = df_device_X[sensor_device]

    print("*************Total for Libe", sum_NaN, "/", sum_Valid)

    SensorSynfield_list = [
        "Synfield_155849.csv",
        "Synfield_155877.csv",
        "Synfield_28843.csv",
        "Synfield_144243.csv",
        "Synfield_28850.csv",
    ]
    sensor_synfield[155849].append([147653])
    sensor_synfield[155877].append([148065])
    sensor_synfield[28843].append([148067])
    sensor_synfield[144243].append([146779])
    sensor_synfield[28850].append([147657])

    sum_NaN = 0
    sum_Valid = 0
    df_sensor_Synfield = pd.DataFrame(columns=[])
    for file_i in SensorSynfield_list:  # FullSites_list:
        df_site_i = pd.read_csv(file_i, delimiter=";", index_col='timestamps', parse_dates=True)
        df_device_X, begin = ETL_outrage(df_site_i)
        siteID = int(file_i.split("_")[1].split(".")[0])
        if len(sensor_synfield[siteID]) == 0:
            continue
        sensor_device = str(sensor_synfield[siteID][0][0])
        sensor_device = [head for head in list(df_device_X) if sensor_device in head]
        df_device_X[df_device_X == 0] = -1
        df_poweron = df_device_X[sensor_device].iloc[begin:]
        print(siteID, sum(df_poweron[df_poweron == -1].count()), df_poweron.shape[0])
        sum_Valid += df_poweron.shape[0]
        sum_NaN += sum(df_poweron[df_poweron == -1].count())
        df_sensor_Synfield[sensor_device] = df_device_X[sensor_device]

    print("*************Total for Synf", sum_NaN, "/", sum_Valid)


    sensor_power[144024].append([
        149025,
        149024,
        149027,
    ])
    sensor_power[27827].append([
        153494,
        153493,
        153490,
    ])

    sensor_power[144242].append([
        156840,
        156839,
        156841,
        153985,
        153984,
        153983
    ])

    sensor_power[155076].append([
        155437,
        155435,
        155439,
        155418,
        155426,
        155416,
        155452,
        155446,
        155448,
        155378,
        155380,
        155391,
        155401,
        155399,
        155394,
        155407,
        155411,
        155409,
    ])

    sensor_power[155851].append([
        153779,
        153781,
        153780,
    ])
    sensor_power[155849].append([
        155551,
        155552,
        155550,
    ])
    sensor_power[155077].append([
        # 155024,
        # 155031,
        # 155038,
        # 154976,
        # 154990,
        # 154983,
        # 155017,
        # 155010,
        # 155003,
        # 155050,
        # 155044,
        # 155042,
        # 155066,
        # 155072,
        # 155060
        155030,
        155033,
        155032,
        154991,
        154984,
        154975,
        155011,
        155018,
        155005,
        155054,
        155057,
        155056
    ])
    sensor_power[155865].append([
        206855,
        206856,
        206857,
    ])
    sensor_power[155877].append([
        155878,
        155880,
        155879,
    ])
    sensor_power[28843].append([
        90841,
        90843,
        90840
    ])
    sensor_power[144243].append([
        147185,
        147183,
        147187,
    ])
    sensor_power[28850].append([
        147503,
        147502,
        147501
    ])

    sensor_power[157185].append([
        155913,
        155915,
        155914,
        155912,
        155916,
        155911,
        205583,
        205582,
        205584,
    ])
    sensor_power[19640].append([
        90496,
        144068,
        144067,
    ])

    df_sensor_Mea = pd.DataFrame(columns=[])
    sum_NaN = 0
    sum_Valid = 0
    begin_at = set()
    for file_i in FullSites_list:
        df_site_i = pd.read_csv(file_i, delimiter=";", index_col='timestamps', parse_dates=True)

        old_head = list(df_site_i)
        siteID = int(file_i.split("_")[0])
        if len(sensor_power[siteID]) == 0:
            continue
        sensor_devices = sensor_power[siteID][0]
        while sensor_devices:
            id = sensor_devices[0]
            sensor_devices.pop(0)
            match1=[head for head in old_head if str(id) in head]
            id = sensor_devices[0]
            sensor_devices.pop(0)
            match2=[head for head in old_head if str(id) in head]
            id = sensor_devices[0]
            sensor_devices.pop(0)
            match3=[head for head in old_head if str(id) in head]
            device_x = match1+match2+match3
            df_device_x  = df_site_i.loc[:,device_x]
            df_device_ETL,begin = ETL_outrage(df_device_x)
            df_device_ETL[df_device_ETL == 0] = -1
            if begin != df_device_ETL.shape[0]:
                begin_at.add(begin)
                device_x_head = list(df_device_ETL)[0]
                df_sensor_Mea[device_x_head]=df_device_ETL[device_x_head]
        begin = min(begin_at)
        df_poweron =df_sensor_Mea.iloc[begin:]
        valid = df_poweron.shape[0]*df_poweron.shape[1]
        outrage = sum(df_poweron[df_poweron == -1].count())
        print(siteID,outrage,valid)
        sum_NaN += outrage
        sum_Valid +=valid
    print("total for Power",sum_NaN,"/",sum_Valid)


    df_list = [df_sensor_Env, df_sensor_Libelium, df_sensor_Synfield, df_sensor_Mea]
    fig, axn = plt.subplots(4, 1, sharex=True, gridspec_kw={'height_ratios': [8, 6, 6, 8]})
    # cbar_ax = fig.add_axes([.92, .3, .03, .4])  # [left, bottom, width, height]

    YLabel_list = ['Environmental', 'Atmospheric', 'Weather', 'Power']
    xticklabels = [int(str(i).split("-")[1]) for i in df_sensor_Env.index.date][2:]
    # xticklabels = [i for i in df_sensor_Env.index.date][2:]
    i = 0
    for j, ax in enumerate(axn.flat):
        df_norm = df_list[i]
        df_norm.index = [pd.to_datetime(str(date)).strftime('%b') for date in df_norm.index.values]
        # df_norm = df_norm.fillna(0)
        # df_norm[df_norm ==0] = 1


        sns.heatmap(df_norm.iloc[2:].T,  # because we have first two items from Oct , just remove it !
                    ax=ax,
                    xticklabels=31,
                    yticklabels=False,
                    cbar=False
                    )
        ax.set_xlabel('')
        ax.set_ylabel(YLabel_list[i], rotation=90, labelpad=5, fontsize=9)
        i += 1

        # axn[-1].set_xticklabels(,xticklabels)
        # axn[-1].set_xlabel('month')

        #
    # plt.savefig('ByType.png',dpi=4000)


    df_total = pd.DataFrame()
    df_total = pd.concat([df_total,df_sensor_Env],axis=1)
    print(df_total.shape)
    df_total = pd.concat([df_total, df_sensor_Libelium], axis=1)
    print(df_total.shape)
    df_total = pd.concat([df_total,df_sensor_Synfield],axis=1)
    print(df_total.shape)
    df_total = pd.concat([df_total,df_sensor_Mea],axis=1)
    print(df_total.shape)


    heads = list(df_total)
    sites = [ site.split("_")[0] for site in heads]
    import collections

    d = collections.defaultdict(list)
    for index, item in enumerate(sites):
        d[item].append(index)
    # print(d)

    dfXXX = []
    id_list = [144024,
               28843,
               144243,
               28850,
               144242,
               19640,
               27827,
               155849,
               155851,
               155076,
               155865,
               155077,
               155877,
               157185,
               159705]
    while id_list:
        k = str(id_list[0])
        v = d[k]
        # print(v)
        df=df_total.iloc[:,v]
        dfXXX.append(df)
        print(df.shape[1])
        id_list.pop(0)


    # df_list= [df_sensor_Env, df_sensor_Libelium, df_sensor_Synfield, df_sensor_Mea]


    fig, axn = plt.subplots(len(dfXXX), 1, sharex=True)#,gridspec_kw={'height_ratios': [8,6,6,8]})
    # cbar_ax = fig.add_axes([.92, .3, .03, .4])  # [left, bottom, width, height]
    site_newname=['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O']

    # YLabel_list = ['Environmental', 'Atmospheric', 'Weather', 'Power']
    xticklabels = [int(str(i).split("-")[1]) for i in df_sensor_Env.index][2:]
    # xticklabels = [i for i in df_sensor_Env.index.date][2:]
    i = 0
    for j, ax in enumerate(axn.flat):
        df_norm = dfXXX[i]
        df_norm.index = [pd.to_datetime(str(date)).strftime('%b') for date in df_norm.index.values]
        # print(sum(df_norm[df_norm == -1].count()))
        # df_norm = df_norm.fillna(0)
        # df_norm[df_norm ==0] = 1


        sns.heatmap(df_norm.iloc[2:].T,  # because we have first two items from Oct , just remove it !
                    ax=ax,
                    xticklabels=31,
                    yticklabels=True,
                    cbar=False
                    )
        ax.set_xlabel('')
        # ax.set_ylabel(YLabel_list[i], rotation=90, labelpad=5,fontsize=9)
        ax.yaxis.set(ticks=[mean(ax.yaxis.get_ticklocs())], ticklabels=site_newname[i])

        i += 1

        # axn[-1].set_xticklabels(,xticklabels)
        # axn[-1].set_xlabel('month')


    # plt.savefig('BySite.png',dpi=4000)
    # plt.show()
    # plt.close('all')

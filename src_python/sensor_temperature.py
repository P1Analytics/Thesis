import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
import warnings
from collections import Counter
import operator
import scipy.stats.stats as stats

warnings.filterwarnings(action="ignore", module="scipy", message="^internal gelsd")


def outliers_Q1_Q3(df, head):
    try:
        Q1 = df.quantile(0.25)
        Q3 = df.quantile(0.75)
        upper = Q3 + (Q3 - Q1) * 3
        lower = Q1 - (Q3 - Q1) * 3
        sub = df.mean()
    except TypeError:
        print("here is the problem of Type Error from: ", head)

    pd.options.mode.chained_assignment = None  # TODO is there a better solution ?
    if df[upper < df].size > 0:
        df[upper < df] = sub
    if df[lower > df].size > 0:
        df[lower > df] = sub
    return df, sub


def outliers_normal_distrib(df):
    return


def comportable(df):
    comfort_zone = df[df > 18 or df < 26]
    print(comfort_zone.size)
    return


def Orient(df):
    # Orientation level 1  only check the day time peak
    df_daytime = df.between_time('6:00', '19:00')
    peak_list = []
    for index, value in df_daytime.nlargest(90).iteritems():
        peak_list.append(index.hour)
    # print(sorted(dict(Counter(peak_list)).items(), key=operator.itemgetter(1), reverse=True)) # Todo debug
    sum = 0
    for hour in peak_list:
        sum += hour / 24 * 360
    Ori = sum / len(peak_list)
    if 0 <= Ori < 90:
        return "North|North-East"
    if 90 <= Ori < 180:
        return "East|South-East"
    if 180 <= Ori < 270:
        return "South|South-West"
    if 270 <= Ori < 359:
        return "West|North-West"
    return "Heaven Or Hell?"


def Slope(df):
    # Orientation level 2 TODO check the slope of the diff
    print(df.diff())
    # slopes = df.diff().div(df.timestamps.diff())
    return


def clean_data(filename, sensor_type=None):
    df = pd.read_csv(filename, delimiter=";", index_col='timestamps', parse_dates=True)
    header = list(df)

    # clear the rows with all 0s due to power-off
    # but this is only for temperature|humidity|? sensor
    if "temperature" == type or "humidity" == sensor_type:
        df = df[(df.T != 0).any()]

    working_sensors = []
    for head in header:
        df_col = df[head]

        if (df_col[df_col != 0]).size == 0:
            # print("this is an broken sensor with all zeros: ", head)
            df = df.drop(head, 1)
            continue

        # clear the outliers for a working sensor <Tukey's fences> aka <interquartile range>
        if "temperature" == type or "humidity" == sensor_type:
            df_col = df_col.replace(0, np.nan)
        df_col, sub = outliers_Q1_Q3(df_col, head)

        # moving window average,smooth out short-term fluctuations and highlight longer-term trends or cycles
        df_col = df_col.rolling(window=30, center=False, min_periods=0).mean()
        df_col = df_col.fillna(sub)

        df[head] = df_col
        working_sensors.append(head)

    df.to_csv(filename.split(".")[0] + "_output.csv", sep=";")
    return df, working_sensors


if __name__ == "__main__":
    # df_indoor, working_indoor = clean_data("19640_Temperature_indoor4.csv")

    df_indoor, working_indoor = clean_data("19640_Temperature_indoor.csv")
    df_outdoor, working_outdoor = clean_data("19640_Temperature_outdoor.csv")
    # df_indoor, working_indoor = clean_data("19640_Luminosity_indoor.csv")
    # df_outdoor, working_outdoor = clean_data("19640_Luminosity_outdoor.csv")

    # df_indoor, working_indoor = clean_data("144243_Temperature_indoor.csv")
    # df_outdoor, working_outdoor = clean_data("144243_Temperature_outdoor.csv")
    # df_indoor, working_indoor = clean_data("27827_Temperature_indoor.csv")
    # df_outdoor, working_outdoor = clean_data("27827_Temperature_outdoor.csv")
    # df_indoor, working_indoor = clean_data("27827_Luminosity_indoor.csv")
    # df_outdoor, working_outdoor = clean_data("27827_Luminosity_outdoor.csv")
    # df_indoor, working_indoor = clean_data("144242_Temperature_indoor.csv")                                           # we have some classrooms facing east
    # df_outdoor, working_outdoor = clean_data("144242_Temperature_outdoor.csv")
    # df_indoor, working_indoor = clean_data("144242_Luminosity_indoor.csv")
    # df_outdoor, working_outdoor = clean_data("144242_Luminosity_outdoor.csv")
    # df_indoor, working_indoor = clean_data("155865_Temperature_indoor.csv")
    # df_outdoor, working_outdoor = clean_data("155865_Temperature_outdoor.csv")
    # df_indoor, working_indoor = clean_data("155865_Luminosity_indoor.csv")
    # df_outdoor, working_outdoor = clean_data("155865_Luminosity_outdoor.csv")
    # df_indoor, working_indoor = clean_data("155877_Temperature_indoor.csv")
    # df_outdoor, working_outdoor = clean_data("155877_Temperature_outdoor.csv")
    # df_indoor, working_indoor = clean_data("144024_Temperature_indoor.csv")
    # df_outdoor, working_outdoor = clean_data("144024_Temperature_outdoor.csv")
    # df_indoor, working_indoor = clean_data("144024_Luminosity_indoor.csv")
    # df_outdoor, working_outdoor = clean_data("144024_Luminosity_outdoor.csv")
    # df_indoor, working_indoor = clean_data("155851_Temperature_indoor.csv")
    # df_outdoor, working_outdoor = clean_data("155851_Temperature_outdoor.csv")
    # df_indoor, working_indoor = clean_data("155851_Luminosity_indoor.csv")
    # df_outdoor, working_outdoor = clean_data("155851_Luminosity_outdoor.csv")

    coef = []
    inter = []
    spearmanr_list = []
    check_ori = True
    for outdoor in working_outdoor:
        X = df_outdoor[outdoor]
        print(outdoor, "\t", Orient(X))
        X = X.values.reshape(np.shape(X)[0], 1)
        for indoor in working_indoor:
            Y = df_indoor[indoor]
            if check_ori:
                print(indoor, "\t", Orient(Y))
            Y = Y.values.reshape(np.shape(Y)[0], 1)
            spearmanr_list.append({outdoor + "/" + indoor: stats.spearmanr(X, Y)[0]})
            print("spearmanr coef for", indoor, "and", outdoor, "is:", stats.spearmanr(X, Y)[0])
            clf = LinearRegression().fit(X, Y)
            print(indoor, outdoor,clf.coef_[0][0], clf.intercept_[0])
            coef.append(clf.coef_[0][0])
            inter.append(clf.intercept_[0])
            xfit = df_indoor[working_indoor[0]].to_dense().values.reshape(np.shape(X)[0], 1)
            yfit = clf.predict(xfit)
            # plt.plot(xfit, yfit, label=indoor)
            # plt.legend(bbox_to_anchor=(1, 1), loc=1, borderaxespad=0)
        check_ori = False

    # print(spearmanr_list)
    plt.figure()
    plt.scatter(coef, inter, marker='.')
    plt.figure()
    df_indoor.boxplot()
    df_indoor.hist()
    plt.show()

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def box_plot_data(df):
    a = df.values.transpose()
    Q0 = min(a)
    Q4 = max(a)
    Q1 = np.percentile(a, 25)
    Q2 = np.percentile(a, 50)
    Q3 = np.percentile(a, 75)
    inner = (Q3-Q1)*1.5
    out = (Q3-Q1)*3
    inner_up = Q3+inner
    inner_down = Q1-inner
    outter_up = Q3 + out
    outter_down = Q1 - out
    print(df.quantile([0,.25, .5, .75,1]))
    # return [outter_down,outter_up,Q0,Q4]

if __name__ == "__main__":
    # csv file first column must be timestamps
    df = pd.read_csv("GREECE.csv", delimiter=";", index_col='timestamps', parse_dates=True)
    header = list(df)

    # first I want to clear all power-off in the data set, one raw are 0s => power-off
    df = df[(df.T != 0).any()]
    # print("the power off period is deleted", df)

    # TODO now the time sequence is not good. Resample by day is necessary but how dose pandas resample/rolling work ?
    df_rolling = df.rolling(window=15, center=True).mean()
    # df_resample = df.resample('W').mean()
    # df_resample = df.resample('D').mean()
    # df_resample.plot()
    # df_resample.to_csv("output1.csv", sep=";")

    working = []
    for head in header:
        df_col = df[head]

        # second I want to clean all the broken sensors , which are all zeros in columns
        # the index for non-all-zeros data is not (0,)
        if np.shape(np.flatnonzero(df_col))[0] != 0:
            working.append(head)

            # TODO third I want to  delete / replace / mean / KNN  those missing data which are zero, tag: temperatures
            # Fill the 0 into NaN , so 0 wont change the statistic data
            df_col = df_col.replace(0, np.nan)

            static = df_col.quantile([0, .25, .5, .75, 1])

            print(df_col.describe()[mean])
            print(static)

            # generally the temperature wont change so much in 10days we use this to fill the missing data
            df_col = df_col.replace(np.nan, method='backfill',limit=10)

            # df_col = df_col.interpolate()
            # plt.figure()
            # df_col.resample('W').mean().plot()

            df[head] = df_col
        else:
            print("this is an broken sensor with all zeros",df_col)

    # plt.show()
    # df.to_csv("output2.csv", sep=";")
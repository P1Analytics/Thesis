import numpy as np
import pandas as pd
from pylab import *
pd.options.mode.chained_assignment = None

def outliers_sliding_window(df, window_number):
    window = []
    outlier = 0
    max_range = 0
    for i in range(df.size):
        item = df[i]
        if len(window) == window_number:
            window.pop(0)
        window.append(item)
        previous = window[:-1]
        if len(previous) == 0:
            continue

        average = mean(previous)
        Q1 = np.percentile(previous, 25)
        Q3 = np.percentile(previous, 75)
        upper = Q3 + (Q3 - Q1) * 3  # 1.5
        lower = Q1 - (Q3 - Q1) * 3  # 1.5
        last_min = np.percentile(previous, 0)
        last_max = np.percentile(previous, 100)
        min = np.percentile(window, 0)
        max = np.percentile(window, 100)
        now_range = max - min
        if max_range < now_range:
            max_range = now_range

        if np.isnan(item) or item == 0:
            outlier += 1
            window[-1] = average  # last_max
            df[i] = average  # last_max

        if len(previous) < window_number - 1:
            continue

        if now_range > max_range * 0.9:  # 0.8:  # new peak comes out, OTHERWISE keep calm and carry on
            outlier += 1
            if item < lower:
                window[-1] = average  # last_min# (last_min + min) / 2
                df[i] = average  # last_min #(last_min + min) / 2
            if item > upper:
                window[-1] = average  # (last_max + max) / 2
                df[i] = average  # last_max #(last_max + max) / 2

    return df, outlier, average


def ETL(df):
    # df = df[~df.index.duplicated(keep='first')]

    for head in list(df):
        df_col = df[head]
        if (df_col[df_col == 0]).size + df_col.isnull().sum() == df_col.size:
            df = df.drop(head, 1)
            continue

    if df.shape[1] > 0:
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
        return df, [], -1

    df_power = df.iloc[begin:]

    df_power[df_power == 0] = np.nan

    for head in list(df_power):
        df_col = df_power[head]
        df_col, outliers, average = outliers_sliding_window(df_col, window_number=10)
        df_col = df_col.rolling(window=6, center=False, min_periods=1).mean()
        df_col = df_col.fillna(average)
        df_power[head] = df_col

    df.iloc[begin:] = df_power
    return df, list(df), begin

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import warnings
from sklearn.linear_model import LinearRegression

warnings.filterwarnings(action="ignore", module="scipy", message="^internal gelsd")



def outliers_Q1_Q3(df, head):
    "return boundary"
    try:
        Q1 = df.quantile(0.25)
        Q3 = df.quantile(0.75)
        upper = Q3 + (Q3 - Q1) * 3
        lower = Q1 - (Q3 - Q1) * 3
        sub = df.mean()
    except TypeError:
        print("here is the problem of Type Error from: ", head)
        print(df)

    return sub, upper, lower



def clean_data(filename):
    df = pd.read_csv(filename, delimiter=";", index_col='timestamps', parse_dates=True)
    header = list(df)
    # first I want to clear all power-off in the data set, one raw are 0s => power-off
    # df = df[(df.T != 0).any()]
    working_sensors = []
    for head in header:
        df_col = df[head]
        # second I want to clean all the broken sensors , which are all zeros in columns
        # the index for non-all-zeros data is not (0,)
        if (df_col[df_col != 0]).size != 0:
            # TODO third I want to  delete / replace / mean / KNN  those missing data which are zero, tag: temperatures
            # df_col = df_col.replace(0, np.nan)
            sub, upper, lower = outliers_Q1_Q3(df_col, head)

            if df_col[df_col > upper].size > 0:
                df_col[df_col > upper] = sub
            if df_col[df_col < lower].size > 0:
                df_col[df_col < lower] = sub

            df_col = df_col.rolling(window=30, center=False, min_periods=0).mean()
            # df_col = df_col.fillna(sub)

            df[head] = df_col
            working_sensors.append(head)
        else:
            print("this is an broken sensor with all zeros called ", head)
            df = df.drop(head, 1)
    df.to_csv(filename.split(".")[0]+"_output.csv", sep=";")

    return df, working_sensors


if __name__ == "__main__":
    df, working = clean_data("27827_motion.csv")
    df_reference, working_reference = clean_data("27827_Noise.csv")
    coef = []
    inter = []
    for ref in working_reference:
        for work in working:
            X = df_reference[ref]
            Y = df[work]
            X = X.values.reshape(np.shape(X)[0], 1)
            Y = Y.values.reshape(np.shape(Y)[0], 1)
            clf = LinearRegression().fit(X, Y)
            print(clf.coef_,clf.intercept_,work,ref)
            coef.append(clf.coef_[0][0])
            inter.append(clf.intercept_[0])
            xfit = df[working[0]].to_dense().values.reshape(np.shape(X)[0],1)
            yfit = clf.predict(xfit)
            # plt.plot(xfit, yfit,label=work)
        break

    plt.figure()
    plt.scatter(coef,inter,marker='.')

    # plt.legend(bbox_to_anchor=(1, 1), loc=1, borderaxespad=0)
    plt.show()



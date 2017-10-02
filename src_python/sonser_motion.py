import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

if __name__ == "__main__":
    # csv file first column must be timestamps
    df = pd.read_csv("motion.csv", delimiter=";", index_col='timestamps', parse_dates=True)
    header = list(df)

    # first I want to clear all power-off in the data set, one raw are 0s => power-off we need another type of data to cross check
    # df = df[(df.T != 0).any()]
    # print("the power off period is deleted", df)

    working = []
    for head in header:
        df_col = df[head]
        # second I want to clean all the broken sensors , which are all zeros in columns
        # the index for non-all-zeros data is not (0,)
        if np.shape(np.flatnonzero(df_col))[0] != 0:
            # TODO third I want to  delete / replace / mean / KNN  those missing data which are zero, tag: motion
            df_col[df_col > 0.0] = 1
            df[head] = df_col
        else:
            print("this is an broken sensor with all zeros or it is not occupied ", head)

    # print(df)
    df_resample = df.resample('D').sum()
    df_resample.plot()
    plt.show()

    df_resample.to_csv("motion_output.csv", sep=";")

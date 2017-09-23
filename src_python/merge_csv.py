import pandas as pd

input_files = ["19640_90496.csv",
               "19640_90603.csv"
               ]
output_file = input_files[0].split("_")[0]+"_Power.csv"

df = pd.read_csv(input_files[0], delimiter=";", names=["timestamps", input_files[0].split(".")[0]])
input_files.pop(0)

while input_files:
    df[input_files[0].split(".")[0]] = pd.read_csv(input_files[0], delimiter=";", names=["timestamps", input_files[0].split(".")[0]],usecols=[1])
    input_files.pop(0)

df.to_csv(output_file,sep=";")
print(df)

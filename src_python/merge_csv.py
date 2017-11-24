import pandas as pd

input_files = ["144242_155928_0xd15_Temperature.csv",
               "144242_155918_0xd1b_Temperature.csv",
               "144242_155924_0x383_Temperature.csv",
               "144242_155933_0xd16_Temperature.csv",
               "144242_155937_0xd18_Temperature.csv",
               "144242_207555_0xd13_Temperature.csv",
               ]
output_file = input_files[0].split("_")[0]+"_Temperatur2months.csv"

df = pd.read_csv(input_files[0], delimiter=";", names=["timestamps", input_files[0].split(".")[0]])
input_files.pop(0)

while input_files:
    df[input_files[0].split(".")[0]] = pd.read_csv(input_files[0], delimiter=";", names=["timestamps", input_files[0].split(".")[0]],usecols=[1])
    input_files.pop(0)

df = df.reset_index(drop=True)
df = df.set_index('timestamps')

df.to_csv(output_file,sep=";")

import pandas as pd

input_files = ["144243_145135_temp_External Temperature.csv",
               "144243_145210_145171_0xd28_Temperature.csv",
               "144243_145210_145239_Temperature_Temperature.csv",
               "144243_145209_145217_Temperature_Temperature.csv",
               "144243_145209_145169_0x192_Temperature.csv",
               "144243_145244_145256_Temperature_Temperature.csv",
               "144243_145244_145230_0xd25_Temperature.csv",
               "144243_145212_145158_0xfe8_Temperature.csv",
               "144243_145212_145227_Temperature_Temperature.csv"
               ]
output_file = input_files[0].split("_")[0]+"_Temperature.csv"

df = pd.read_csv(input_files[0], delimiter=";", names=["timestamps", input_files[0].split(".")[0]])
input_files.pop(0)

while input_files:
    df[input_files[0].split(".")[0]] = pd.read_csv(input_files[0], delimiter=";", names=["timestamps", input_files[0].split(".")[0]],usecols=[1])
    input_files.pop(0)

df.to_csv(output_file,sep=";")

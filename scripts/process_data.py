# This script is to process the data obtained from running the Hadoop MapReduce job.
# Each line is a key-value pair, where the key is the ID-YEAR-REGION_TYPE of the observation
# and the values is AVERAGE-MAX-MIN-STD-MEDIAN of the observations for the station in that year.
# The script will split the key and values and save them into a csv file.

import os
import pandas as pd
from tqdm import tqdm


# check if the file exists
FILE_PATH = "shared/reduced_data/part-r-00000"
if not os.path.isfile(FILE_PATH):
    print("File not found!")
    exit(1)

# create an empty dataframe to hold the values
df = pd.DataFrame(columns=["ID", "YEAR", "REGION_TYPE",
                  "AVERAGE", "MAX", "MIN", "STD", "MEDIAN"])

# read the file line by line
with open(FILE_PATH, "r") as f:

    for line in tqdm(f):

        # split the line into key and values
        key, values = line.split("\t")

        # split the key into ID, YEAR, REGION_TYPE
        key = key.split("-")

        # split the values into AVERAGE, MAX, MIN, STD, MEDIAN
        values = values.split("-")

        # create a dictionary to hold the values
        data = {
            "ID": key[0],
            "YEAR": key[1],
            "REGION_TYPE": key[2],
            "AVERAGE": values[0],
            "MAX": values[1],
            "MIN": values[2],
            "STD": values[3],
            "MEDIAN": values[4]
        }

        # append the data to the dataframe
        df = pd.concat([df, pd.DataFrame(data, index=[0])], ignore_index=True)

# save the dataframe to a csv file
df.to_csv("data/processed_data.csv", index=False)

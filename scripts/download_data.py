import requests
import os
from tqdm import tqdm
from io import BytesIO
import gzip


# regions
HUMID = [601000, 601050, 601060, 601600, 601400]
SEMI_ARID = [601150, 601560, 601460, 601650, 601910]
ARID = [602300, 601780, 602051, 602700, 602650]
HYPER_ARID = [600330, 600340, 602750, 602890, 602890]
regions = [HUMID, SEMI_ARID, ARID, HYPER_ARID]

# start and end years
START_YEAR = 1973
END_YEAR = 2023

BASE_URL = "https://www1.ncdc.noaa.gov/pub/data/noaa"
TOTAL_FILES = END_YEAR - START_YEAR + 1

# make sure the folder to hold the downloaded files exists
if not os.path.exists("./downloads"):
    os.makedirs("./downloads")

# download the files
print("Downloading files...")
for region in regions:

    print(f"Climate: {region}")

    for code in region:

        # keep track of the downloaded and skipped files
        downloaded = 0
        skipped = 0

        print(f"Downloading files for {code}...")
        for i in tqdm(range(START_YEAR, END_YEAR+1)):

            file_name = f"{code}-99999-{i}.gz"
            file_link = f"{BASE_URL}/{i}/{file_name}"

            # if the file already exists, skip
            if os.path.exists(f"./downloads/{file_name}"):
                downloaded += 1
                continue

            try:
                response = requests.get(file_link)
                content = BytesIO(response.content)
            except:
                skipped += 1
                continue

            # extract the data
            with gzip.GzipFile(fileobj=content) as f:
                extracted_data = f.read()

            # write to new file
            with open(f"./downloads/{file_name[:-3]}", "wb") as f:
                f.write(extracted_data)

            downloaded += 1

        print(
            f"Successfully downloaded {downloaded} files out of {TOTAL_FILES}...")
        print(f"Skipped {skipped} files out of {TOTAL_FILES}...")

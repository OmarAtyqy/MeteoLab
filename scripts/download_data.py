import requests
import os
from tqdm import tqdm
from io import BytesIO
import gzip


# climates
HUMID = {'name': 'HUMID', 'list': [601000, 601050, 601060, 601600, 601400]}
SEMI_ARID = {'name': 'SEMI_ARID', 'list': [
    601150, 601560, 601460, 601650, 601910]}
ARID = {'name': 'ARID', 'list': [602300, 601780, 602051, 602700, 602650]}
HYPER_ARID = {'name': 'HYPER_ARID', 'list': [
    600330, 600340, 602750, 602890, 602890]}

# add the new climate to the list of climates
climates = [HUMID, SEMI_ARID, ARID, HYPER_ARID]

# start and end years
START_YEAR = 1943
END_YEAR = 2023

BASE_URL = "https://www1.ncdc.noaa.gov/pub/data/noaa"
TOTAL_YEARS = END_YEAR - START_YEAR + 1

# make sure the folder to hold the downloaded files exists
if not os.path.exists("./downloads"):
    os.makedirs("./downloads")

# keep track of the number of files downloaded and skipped
downloaded = 0
skipped_total = 0

# download the files
print("Downloading files...")
for climate in climates:

    for code in climate['list']:

        # keep track of the number of files downloaded and skipped for each code
        code_skips = 0
        code_downloads = 0

        print(f"Downloading files for {code}...")

        skipped_years_doesnt_exist_list = []
        skipped_years_already_downloaded_list = []

        for i in tqdm(range(START_YEAR, END_YEAR+1)):

            file_name = f"{code}-99999-{i}.gz"
            file_link = f"{BASE_URL}/{i}/{file_name}"

            # if the file already exists, skip
            if os.path.exists(f"./downloads/{file_name}"):
                skipped_total += 1
                code_skips += 1
                skipped_years_already_downloaded_list.append(i)
                continue

            try:
                response = requests.get(file_link)
                content = BytesIO(response.content)

                # extract the data
                with gzip.GzipFile(fileobj=content) as f:
                    extracted_data = f.read()
            except:
                skipped_years_doesnt_exist_list.append(i)
                skipped_total += 1
                code_skips += 1
                continue

            # write to new file, add climate to file name
            file_name = f"{climate['name']}-{file_name}"
            with open(f"./downloads/{file_name[:-3]}", "wb") as f:
                f.write(extracted_data)

            downloaded += 1
            code_downloads += 1

        print(f"Total files downloaded for {code}: {code_downloads}")
        print(
            f"Skipped years that don't exist: {len(skipped_years_doesnt_exist_list)} of {TOTAL_YEARS}")
        print(skipped_years_doesnt_exist_list)
        print(
            f"Skipped years that are already downloaded: {len(skipped_years_already_downloaded_list)} of {TOTAL_YEARS}")
        print(skipped_years_already_downloaded_list)

print("Download completed!")
print(f"Total files downloaded: {downloaded}")
print(f"Total files skipped: {skipped_total}")

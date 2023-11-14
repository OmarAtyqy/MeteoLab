# This script is used to visualize the data in the dataset.
# THE COLUMNS IN THE DATAFRAME ARE:
# ID, YEAR, REGION_TYPE, AVERAGE, MAX, MIN, STD, MEDIAN

import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.cluster.hierarchy import dendrogram, linkage, fcluster
from sklearn.metrics import pairwise_distances
from sklearn.metrics import davies_bouldin_score, silhouette_score
import numpy as np


# check if the file exists
FILE_PATH = "data/processed_data.csv"
if not os.path.exists(FILE_PATH):
    print("File does not exist!")
    exit()

# read the data
df = pd.read_csv(FILE_PATH, index_col=False)

# seperate the data into four groups by region
df_arid = df[df["REGION_TYPE"] == "ARID"]
df_humid = df[df["REGION_TYPE"] == "HUMID"]
df_hyper_arid = df[df["REGION_TYPE"] == "HYPER_ARID"]
df_semi_arid = df[df["REGION_TYPE"] == "SEMI_ARID"]

# put the four groups into a dic with the region type as the key
data = {
    "ARID": df_arid,
    "HUMID": df_humid,
    "HYPER ARID": df_hyper_arid,
    "SEMI ARID": df_semi_arid
}

# # print the number of observations in each group
# print("ARID:", len(df_arid))
# print("HUMID:", len(df_humid))
# print("HYPER_ARID:", len(df_hyper_arid))
# print("SEMI_ARID:", len(df_semi_arid))

# # plot the evolution of the average temperature over the years for each region type into one plot
# sns.set_style("whitegrid")
# sns.set_palette("Set2")
# fig, ax = plt.subplots(figsize=(12, 8))
# for region_type, df in data.items():
#     sns.lineplot(x="YEAR", y="AVERAGE", data=df, label=region_type, ax=ax)
# plt.legend()
# plt.title("Evolution of the average temperature over the years for each region type")
# plt.show()

# # plot the evolution of the max temperature over the years for each region type into one plot
# sns.set_style("whitegrid")
# sns.set_palette("Set2")
# fig, ax = plt.subplots(figsize=(12, 8))
# for region_type, df in data.items():
#     sns.lineplot(x="YEAR", y="MAX", data=df, label=region_type, ax=ax)
# plt.legend()
# plt.title("Evolution of the max temperature over the years for each region type")
# plt.show()

# # plot the evolution of the min temperature over the years for each region type into one plot
# sns.set_style("whitegrid")
# sns.set_palette("Set2")
# fig, ax = plt.subplots(figsize=(12, 8))
# for region_type, df in data.items():
#     sns.lineplot(x="YEAR", y="MIN", data=df, label=region_type, ax=ax)
# plt.legend()
# plt.title("Evolution of the min temperature over the years for each region type")
# plt.show()

# # plot the evolution of the median of the temperature over the years for each region type into one plot
# sns.set_style("whitegrid")
# sns.set_palette("Set2")
# fig, ax = plt.subplots(figsize=(12, 8))
# for region_type, df in data.items():
#     sns.lineplot(x="YEAR", y="MEDIAN", data=df, label=region_type, ax=ax)
# plt.legend()
# plt.title(
#     "Evolution of the median of the temperature over the years for each region type")
# plt.show()

# # plot the evolution of the standard deviation of the temperature over the years for each region type into one plot
# sns.set_style("whitegrid")
# sns.set_palette("Set2")
# fig, ax = plt.subplots(figsize=(12, 8))
# for region_type, df in data.items():
#     sns.lineplot(x="YEAR", y="STD", data=df, label=region_type, ax=ax)
# plt.legend()
# plt.title(
#     "Evolution of the standard deviation of the temperature over the years for each region type")
# plt.show()

# use clustering algorithms to cluster each of the dataframes into 2 clusters
# using scipy Linkage and the ward method
n_clusters = 2
scores = {}
for region_type, df in data.items():

    # save the year, month and day from the dataframe
    year = df["YEAR"]
    month = df["MONTH"]
    day = df["DAY"]

    # drop the year, month and day from the dataframe
    df = df.drop(["MONTH", "REGION_TYPE", "YEAR", "DAY"], axis=1)

    # create a linkage matrix
    Z = linkage(df, method="ward")

    # assign labels to each day based on the linkage matrix
    labels = fcluster(Z, t=n_clusters, criterion='maxclust')

    # add the labels to the dataframe
    df["CLUSTER"] = labels

    # add the year, month and day back to the dataframe
    df["YEAR"] = year
    df["MONTH"] = month
    df["DAY"] = day

    # save the dataframe to a csv file
    df.to_csv(f"data/clustered_data_{region_type}.csv", index=False)

    # compute the Silhouette and Davis-Bouldin scores
    X = df[['AVERAGE', 'MAX', 'MIN', 'STD', 'MEDIAN']]
    y = df['CLUSTER']

    # Calculate inter-cluster distance
    inter_cluster_distance = pairwise_distances(X, metric='euclidean')

    # Calculate intra-cluster distance
    intra_cluster_distance = []
    unique_labels = y.unique()

    for label in unique_labels:
        # Extract the data points belonging to the current label
        label_points = X[y == label]
        if len(label_points) > 1:
            # Calculate the intra-cluster distance for the current label
            distance = pairwise_distances(
                label_points, metric='euclidean').mean()
            intra_cluster_distance.append(distance)

    # Calculate Davies-Bouldin Index
    davies_bouldin_index = davies_bouldin_score(X, y)

    # Calculate Silhouette Score
    silhouette_score_avg = silhouette_score(X, y)

    # save the scores to a dictionary
    scores[region_type] = {
        "Region Type": region_type,
        "Silhouette Score": silhouette_score_avg,
        "Davies-Bouldin Index": davies_bouldin_index,
        "Inter-cluster distance": inter_cluster_distance.mean(),
        "Intra-cluster distance": np.mean(intra_cluster_distance)
    }

# save the scores to a csv file
scores_df = pd.DataFrame(scores).T
scores_df.to_csv("data/clustering_scores.csv", index=False)

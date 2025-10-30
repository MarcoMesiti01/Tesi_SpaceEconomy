from sklearn.cluster import DBSCAN
import pandas as pd
import Library as mylib
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score

dbscan=DBSCAN(eps=0.8, min_samples=5)
dfNorm=pd.read_parquet("Tesi_SpaceEconomy\Clustering\DimDataCluster.parquet")
df=pd.read_parquet("Tesi_SpaceEconomy\Clustering\DimDataClusterNoNorm.parquet")

labels=dbscan.fit_predict(dfNorm.fillna(0))
dfNorm["cluster"]=labels

df=pd.merge(left=df, right=dfNorm["cluster"], how="left", left_index=True, right_index=True)
df=df.groupby(by="cluster").mean()
df.to_excel("Tesi_SpaceEconomy\Clustering\OutputCluster.xlsx")
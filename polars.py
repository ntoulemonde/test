import os
import polars as pl
import s3fs
from pynsee.download import download_file

pandas_df_bpe = download_file("BPE_ENS", update = True)
pandas_df_bpe.__class__
df = pl.from_pandas(pandas_df_bpe)

S3_ENDPOINT_URL = "https://" + os.environ["AWS_S3_ENDPOINT"]
fs = s3fs.S3FileSystem(client_kwargs={'endpoint_url': S3_ENDPOINT_URL})

with fs.open("donnees-insee/diffusion/BPE/2019/BPE_ENS.csv") as bpe_csv:
    df_bpe = pl.read_csv(bpe_csv, separator = ";", 
    schema_overrides={
        "DEP": pl.Categorical,
        "DEPCOM": pl.Categorical
    })

df_bpe.lazy().filter(
    pl.col("TYPEQU") == "B316"
).group_by(
    "DEP"
).agg(
    pl.sum("NB_EQUIP").alias("NB_STATION_SERVICE")
).collect()

df_bpe.select(
    ["DEPCOM", "TYPEQU", "NB_EQUIP"]
)

df_bpe.select(
    pl.col("^D.*$")
).head(5)

cols_lower = {cols: cols.lower() for cols in df_bpe.columns}

df_bpe.rename(cols_lower)
df_bpe\
    .filter(pl.col("TYPEQU") == "B316")\
    .with_columns(pl.col("NB_EQUIP").cast(pl.Int64, strict=False))\
    .with_columns(
        pl.col("NB_EQUIP").cum_sum().alias("NB_EQUIP_SUM")
    )

df_bpe.rename(cols_lower)
df_bpe\
    .group_by("DEP")\
    .agg((pl.col("TYPEQU") == "B203").sum().alias("NB_BOULANGERIES_TOT"))\
    .sort("NB_BOULANGERIES_TOT", descending=True)

df_bpe\
    .filter(pl.col("TYPEQU") == "B203")\
    .group_by("DEP")\
    .agg(pl.col("TYPEQU").count().alias("NB_BOULANGERIES_TOT"))\
    .sort("NB_BOULANGERIES_TOT", descending=True)

LazyFrame = df_bpe.lazy()\
    .with_columns(pl.col('NB_EQUIP').cast(pl.Int64, strict=False))\
    .select(
        pl.col("NB_EQUIP").sum().alias("NB_EQUIP_TOT")
    )

LazyFrame.show_graph()


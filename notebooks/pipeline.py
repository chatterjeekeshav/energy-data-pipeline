# Reset tables if they exist
spark.sql("DROP TABLE IF EXISTS bronze_energy")
spark.sql("DROP TABLE IF EXISTS silver_energy")
spark.sql("DROP TABLE IF EXISTS gold_energy")

# =========================
# READ DATA
# =========================
df = spark.table("owid_energy_data")

# =========================
# BRONZE
# =========================
df.write.mode("overwrite").saveAsTable("bronze_energy")

# =========================
# SILVER
# =========================
from pyspark.sql.functions import col, when

bronze_df = spark.table("bronze_energy")

silver_df = (
    bronze_df
    .withColumn("year", col("year").cast("int"))
    .withColumn("primary_energy_consumption", col("primary_energy_consumption").cast("double"))
    .withColumn("electricity_generation", col("electricity_generation").cast("double"))
    .withColumn("renewables_share_energy", col("renewables_share_energy").cast("double"))
    .dropna(subset=["country", "year"])
    .withColumn(
        "energy_category",
        when(col("renewables_share_energy") >= 50, "GREEN")
        .when(col("renewables_share_energy") >= 20, "HYBRID")
        .otherwise("FOSSIL")
    )
)

silver_df.write.mode("overwrite").saveAsTable("silver_energy")

# =========================
# GOLD
# =========================
from pyspark.sql.functions import avg

gold_df = silver_df.groupBy("country", "energy_category").agg(
    avg("primary_energy_consumption").alias("avg_energy_consumption"),
    avg("electricity_generation").alias("avg_electricity_generation"),
    avg("renewables_share_energy").alias("avg_renewables_share")
)

gold_df.write.mode("overwrite").saveAsTable("gold_energy")
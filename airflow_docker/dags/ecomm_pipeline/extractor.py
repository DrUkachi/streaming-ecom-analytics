import os
import json
import requests
import gzip
import shutil
from datetime import datetime
from dateutil.relativedelta import relativedelta

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# Constants
CHECKPOINT_PATH = "checkpoint_ecommerce_rotating.json"
MONTHS_ORDERED = ["2019-Oct", "2019-Nov", "2019-Dec", "2020-Jan", "2020-Feb", "2020-Mar", "2020-Apr"]
BASE_URL = "https://data.rees46.com/datasets/marketplace/"
DATA_FOLDER = "/content/drive/MyDrive/Career Development/Data Engineering/Zoomcamp/starter-codes"

class MonthlyExtractor:
    def __init__(self):
        self.spark = SparkSession.builder.appName("MonthlyEventExtractor").getOrCreate()
        self.checkpoint = self._load_checkpoint()
        self.current_month_index = MONTHS_ORDERED.index(self.checkpoint["current_month"])
        print(f"✅ Loaded checkpoint: {self.checkpoint}")

    def _load_checkpoint(self):
        try:
            with open(CHECKPOINT_PATH, "r") as f:
                data = json.load(f)
                assert "current_month" in data and "global_offset" in data
                return data
        except Exception as e:
            print(f"⚠️ Failed to load checkpoint, using defaults. Reason: {e}")
            return {
                "current_month": MONTHS_ORDERED[0],
                "global_offset": 100000
            }

    def _save_checkpoint(self):
        with open(CHECKPOINT_PATH, "w") as f:
            json.dump(self.checkpoint, f)
        print(f"💾 Saved checkpoint: {self.checkpoint}")

    def download_and_unzip(self, month_str):
        """
        Download the .csv.gz file for the given month if not exists,
        then unzip it to .csv.
        """
        # Parse year and month abbreviation from month_str (e.g., "2019-Oct")
        year, month_abbr = month_str.split("-")
        filename_gz = f"{year}-{month_abbr}.csv.gz"
        filename_csv = f"{year}-{month_abbr}.csv"

        gz_path = os.path.join(DATA_FOLDER, filename_gz)
        csv_path = os.path.join(DATA_FOLDER, filename_csv)

        # Ensure data folder exists
        os.makedirs(DATA_FOLDER, exist_ok=True)

        if os.path.exists(csv_path):
            print(f"✅ CSV already exists: {csv_path}")
            return csv_path

        # Download if .gz does not exist
        if not os.path.exists(gz_path):
            url = f"{BASE_URL}{filename_gz}"
            print(f"📥 Downloading {filename_gz} from {url} ...")
            try:
                response = requests.get(url)
                response.raise_for_status()
                with open(gz_path, "wb") as f_out:
                    f_out.write(response.content)
                print(f"✅ Downloaded: {gz_path}")
            except requests.exceptions.RequestException as e:
                print(f"❌ Failed to download {filename_gz}: {e}")
                return None
        else:
            print(f"✅ .gz file already exists: {gz_path}")

        # Unzip .gz to .csv
        try:
            with gzip.open(gz_path, 'rb') as f_in, open(csv_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            print(f"🗃️ Unzipped to: {csv_path}")
            return csv_path
        except Exception as e:
            print(f"❌ Failed to unzip {gz_path}: {e}")
            return None

    def extract(self):
        month = self.checkpoint["current_month"]
        global_offset = self.checkpoint["global_offset"]

        print(f"\n🚀 Starting extraction for {month} | Global Offset: {global_offset}")

        # Ensure CSV exists, download if missing
        csv_path = self.download_and_unzip(month)
        if csv_path is None or not os.path.exists(csv_path):
            print(f"❌ Cannot proceed, CSV file missing for {month}. Skipping extraction.")
            return None

        # Read CSV with Spark
        df = self.spark.read.csv(csv_path, header=True)

        # Data preprocessing
        processed_df = (
            df.withColumn("event_time", F.to_timestamp("event_time"))
              .withColumn("event_date", F.to_date("event_time"))
              .withColumn("product_id", F.col("product_id").cast("string"))
              .withColumn("category_id", F.col("category_id").cast("string"))
              .withColumn("user_id", F.col("user_id").cast("string"))
              .withColumn("user_session", F.col("user_session").cast("string"))
              .withColumn("brand", F.when(F.col("brand").isNotNull(), F.col("brand")).otherwise(None))
              .withColumn("category_code", F.when(F.col("category_code").isNotNull(), F.col("category_code")).otherwise(None))
              .withColumn("price", F.coalesce(F.col("price").cast("double"), F.lit(0.0)))
        )

        # Optimized processing using window functions
        window_spec = Window.partitionBy("event_date").orderBy("event_time")
        limited_df = (
            processed_df
            .withColumn("row_num", F.row_number().over(window_spec))
            .filter(F.col("row_num") <= 10000)
            .drop("row_num")
        )

        # Get date statistics
        date_stats = limited_df.groupBy("event_date").agg(F.count("*").alias("event_count"))
        date_stats.persist()  # Cache for multiple actions
        total_rows = date_stats.agg(F.sum("event_count")).first()[0] or 0
        unique_dates = date_stats.count()

        print(f"📅 Dates in month with data: {unique_dates}")
        print(f"📦 Total events collected: {total_rows}")

        # Write all data at once
        output_path = os.path.join(DATA_FOLDER, f"{month}-output.parquet")
        if total_rows > 0:
            limited_df.write.mode("overwrite").parquet(output_path)
            print(f"💾 Data written to {output_path}")
        else:
            print(f"⚠️ No data to write for {month}")

        # Update checkpoint
        self.checkpoint["global_offset"] += total_rows

        if self.current_month_index < len(MONTHS_ORDERED) - 1:
            self.current_month_index += 1
            self.checkpoint["current_month"] = MONTHS_ORDERED[self.current_month_index]
        else:
            self.current_month_index = 0
            self.checkpoint["current_month"] = MONTHS_ORDERED[0]
            self.checkpoint["global_offset"] += 10000

        self._save_checkpoint()

        print(f"\n✅ Finished extracting {month}")
        print(f"🔄 Next month: {self.checkpoint['current_month']}")
        print(f"📊 Global offset: {self.checkpoint['global_offset']}")

        return {
            "output_path": output_path,
            "event_count": total_rows,
            "processed_month": month
        }

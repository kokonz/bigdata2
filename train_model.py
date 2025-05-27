from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
import shutil
import sys
import os

spark = SparkSession.builder.appName("TaxiModel").getOrCreate()

try:
    for i in range(1, 4):
        files = [f'batch_{j}.csv' for j in range(1, i+1)]
        
        for file in files:
            if not os.path.exists(file):
                raise FileNotFoundError(f"File {file} tidak ditemukan!")
        
        df = spark.read.csv(files, header=True, inferSchema=True)
        
        # Preprocessing
        df_clean = df.filter(
            (df.pickup_longitude.between(-180, 180)) &
            (df.pickup_latitude.between(-90, 90))
        )
        
        if df_clean.count() == 0:
            raise ValueError(f"Data di {files} kosong setelah filter!")
        
        assembler = VectorAssembler(
            inputCols=["pickup_longitude", "pickup_latitude"],
            outputCol="features"
        )
        data = assembler.transform(df_clean)
        
        kmeans = KMeans(k=3, seed=42)
        model = kmeans.fit(data)
        
        model_path = f"model_v{i}"
        shutil.rmtree(model_path, ignore_errors=True)
        model.save(model_path)
        print(f"Model {i} trained dengan {df_clean.count():,} records")

except Exception as e:
    print(f"Error: {str(e)}")
    sys.exit(1)

finally:
    spark.stop()
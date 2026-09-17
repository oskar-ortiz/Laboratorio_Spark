from pathlib import Path
import os

from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATASET_PATH = PROJECT_ROOT / "data" / "raw" / "online_retail.csv"


def main() -> None:
    if not DATASET_PATH.exists():
        raise FileNotFoundError(f"No se encontro el dataset: {DATASET_PATH}")

    spark = (
        SparkSession.builder
        .appName("LaboratorioSparkSmokeTest")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", str(PROJECT_ROOT / "data" / "spark-warehouse"))
        .getOrCreate()
    )
    try:
        data = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .option("encoding", "UTF-8")
            .csv(str(DATASET_PATH))
        )
        data.printSchema()
        data.show(5, truncate=False)
        print(f"Filas leidas: {data.count()}")
        print(f"Spark version: {spark.version}")
        print(f"Python: {os.environ.get('PYSPARK_PYTHON', 'no configurado')}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

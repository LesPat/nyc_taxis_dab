import argparse
from databricks.sdk.runtime import spark
from nyc_taxis_dab import taxis


def main():
    parser = argparse.ArgumentParser(description="NYC taxi ETL job")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--taxi-s3-path", required=True)
    args = parser.parse_args()

    spark.sql(f"USE CATALOG {args.catalog}")
    spark.sql(f"USE SCHEMA {args.schema}")

    taxis.load_taxi_fares(args.taxi_s3_path).printSchema()


if __name__ == "__main__":
    main()

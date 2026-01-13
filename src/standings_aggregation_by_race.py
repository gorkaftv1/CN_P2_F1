# driver_standings_aggregation_by_race.py
import sys
import logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, sum as spark_sum, count, countDistinct, first, when
from pyspark.sql.window import Window
from awsglue.dynamicframe import DynamicFrame

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    args = getResolvedOptions(sys.argv, ['database', 'table', 'output_path'])
    database = args['database']
    table = args['table']
    output_path = args['output_path']
    
    logger.info(f"Database: {database}, Table: {table}, Output: {output_path}")
    
    sc = SparkContext()
    glueContext = GlueContext(sc)
    
    # Leer desde Glue Catalog usando GlueContext
    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table
    )
    
    # Convertir a Spark DataFrame
    df = dynamic_frame.toDF()
    df.printSchema()
    logger.info(f"Registros leídos: {df.count()}")
    
    # Para cada raceId, contar cuántos position=1 tiene cada piloto
    # y obtener el piloto con más victorias (position=1)
    df_with_wins = df.withColumn(
        "is_winner",
        when(col("position") == 1, 1).otherwise(0)
    )
    
    # Agregación por raceId y driverId para contar victorias
    wins_by_driver = df_with_wins.groupBy("raceId", "driverId", "forename", "surname") \
        .agg(
            spark_sum("is_winner").alias("num_victories")
        )
    
    # Para cada raceId, obtener el piloto con más victorias
    window = Window.partitionBy("raceId").orderBy(col("num_victories").desc())
    from pyspark.sql.functions import row_number
    
    top_driver = wins_by_driver \
        .withColumn("rank", row_number().over(window)) \
        .filter(col("rank") == 1) \
        .select("raceId", "driverId", "forename", "surname", "num_victories")
    
    # Agregación general por carrera (raceId)
    race_df = df.groupBy("raceId") \
        .agg(
            countDistinct("driverId").alias("total_drivers"),
            spark_sum("points").alias("total_points"),
            count("*").alias("total_records")
        )
    
    # Join con el piloto con más victorias
    result_df = race_df.join(top_driver, "raceId", "left") \
        .select(
            "raceId",
            "total_drivers",
            "total_points",
            "total_records",
            col("driverId").alias("top_driver_id"),
            col("forename").alias("top_driver_forename"),
            col("surname").alias("top_driver_surname"),
            col("num_victories").alias("top_driver_victories")
        ) \
        .orderBy("raceId")
    
    output_dynamic_frame = DynamicFrame.fromDF(result_df, glueContext, "output")
    
    logger.info(f"Registros agregados: {output_dynamic_frame.count()}")
    
    # Escribir usando GlueContext con particiones por raceId
    glueContext.write_dynamic_frame.from_options(
        frame=output_dynamic_frame,
        connection_type="s3",
        connection_options={
            "path": output_path,
            "partitionKeys": ["raceId"]
        },
        format="parquet",
        format_options={"compression": "snappy"}
    )
    
    logger.info(f"Completado. Registros: {result_df.count()}")

if __name__ == "__main__":
    main()

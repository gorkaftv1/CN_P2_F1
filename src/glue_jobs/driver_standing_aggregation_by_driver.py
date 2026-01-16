import sys
import logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, min as spark_min, max as spark_max, avg, count, stddev
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
    
    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table
    )
    
    df = dynamic_frame.toDF()
    df.printSchema()
    logger.info(f"Registros leídos: {df.count()}")
    
    # Agregación por piloto: estadísticas de cada driverId
    # Usamos first() para obtener forename y surname (son constantes por driverId)
    from pyspark.sql.functions import first, sum as spark_sum
    
    driver_df = df.groupBy("driverId") \
        .agg(
            first("forename").alias("forename"),
            first("surname").alias("surname"),
            spark_min("position").alias("mejor_posicion_historica"),
            avg("position").alias("posicion_promedio_historica"),
            stddev("position").alias("desviacion_estandar_posicion"),
            count("raceId").alias("num_races"),
            spark_sum(col("points")).alias("total_points"),
            avg("points").alias("avg_points")
        ) \
        .orderBy("driverId")
    
    output_dynamic_frame = DynamicFrame.fromDF(driver_df, glueContext, "output")
    
    logger.info(f"Registros agregados: {output_dynamic_frame.count()}")
    
    # Escribir sin particionKeys ya que driverId es la clave de agregación
    glueContext.write_dynamic_frame.from_options(
        frame=output_dynamic_frame,
        connection_type="s3",
        connection_options={
            "path": output_path
        },
        format="parquet",
        format_options={"compression": "snappy"}
    )
    
    logger.info(f"Completado. Registros: {driver_df.count()}")

if __name__ == "__main__":
    main()

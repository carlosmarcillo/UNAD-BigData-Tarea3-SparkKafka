from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, count, round

def main():
    # 1. Inicializar la sesión de Spark para procesamiento Batch
    spark = SparkSession.builder \
        .appName("AnalisisBatchBigData") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("=== Iniciando Procesamiento Batch con PySpark ===")

    # 2. Cargar el conjunto de datos (ejemplo con datos simulados o archivo CSV/JSON)
    # Si tienes un archivo CSV, usa: df = spark.read.csv("ruta/al/dataset.csv", header=True, inferSchema=True)
    data = [
        (1, "Sensor_Norte", "Operativo", 24.5, 55.0, "2026-10-01"),
        (2, "Sensor_Sur", "Operativo", 28.1, 45.2, "2026-10-01"),
        (3, "Sensor_Este", "Mantenimiento", 21.0, 68.4, "2026-10-01"),
        (4, "Sensor_Oeste", "Operativo", 29.8, 40.1, "2026-10-01"),
        (5, "Sensor_Norte", "Operativo", 25.2, 53.8, "2026-10-02"),
        (6, "Sensor_Sur", "Operativo", 27.6, 47.0, "2026-10-02"),
        (7, "Sensor_Este", "Operativo", 22.4, 65.0, "2026-10-02"),
        (8, "Sensor_Oeste", "Mantenimiento", 30.1, 38.5, "2026-10-02")
    ]
    
    columns = ["id", "ubicacion", "estado", "temperatura", "humedad", "fecha"]
    df = spark.createDataFrame(data, schema=columns)

    print("\n--- Esquema del Dataset ---")
    df.printSchema()

    print("\n--- Vista Previa de los Datos Cargados ---")
    df.show()

    # 3. Limpieza y Filtrado de Datos
    print("\n--- Filtrando solo Sensores en estado 'Operativo' ---")
    df_operativos = df.filter(col("estado") == "Operativo")
    df_operativos.show()

    # 4. Transformaciones y Análisis Exploratorio de Datos (EDA)
    print("\n--- Métricas Estadísticas por Ubicación (Promedio, Máximo, Mínimo) ---")
    df_analisis = df_operativos.groupBy("ubicacion") \
        .agg(
            count("id").alias("total_registros"),
            round(avg("temperatura"), 2).alias("temp_promedio"),
            max("temperatura").alias("temp_maxima"),
            min("temperatura").alias("temp_minima"),
            round(avg("humedad"), 2).alias("humedad_promedio")
        ) \
        .orderBy("temp_promedio", ascending=False)

    df_analisis.show()

    # 5. Finalizar sesión
    spark.stop()
    print("=== Procesamiento Batch Finalizado con Éxito ===")

if __name__ == "__main__":
    main()

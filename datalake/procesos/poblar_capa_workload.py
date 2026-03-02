#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script PySpark para despliegue de capa Workload - Proyecto Telco Churn

"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# =============================================================================
# @section 1. Configuración de parámetros
# =============================================================================

def parse_arguments():
    parser = argparse.ArgumentParser(description='Proceso de carga - Capa Workload Churn')
    parser.add_argument('--env', type=str, default='TopicosA', help='Entorno: DEV, QA, PROD')
    parser.add_argument('--username', type=str, default='hadoop', help='Usuario HDFS')
    parser.add_argument('--base_path', type=str, default='/user', help='Ruta base en HDFS')
    # Ajustado a tu ruta actual
    parser.add_argument('--data_path', type=str, 
                        default='/user/hadoop/dataset', 
                        help='Ruta de los datos en HDFS (archivo ya copiado con hdfs dfs -put)')
    return parser.parse_args()

# =============================================================================
# @section 2. Inicialización de SparkSession
# =============================================================================

def create_spark_session(app_name="Proceso_Carga_Workload_Churn"):
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false") \
        .config("spark.sql.legacy.charVarcharCodegen", "true") \
        .getOrCreate()

# =============================================================================
# @section 3. Funciones auxiliares (Lógica del docente)
# =============================================================================

def crear_database(spark, env, username, base_path):
    db_name = f"{env}_workload"
    db_location = f"{base_path}/{username}/datalake/{db_name}"
    
    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{db_location}'")
    print(f"✅ Database '{db_name}' creada en: {db_location}")
    return db_name

def crear_tabla_external(spark, db_name, table_name, df, location, spark_schema):
    df.createOrReplaceTempView(f"tmp_{table_name}")
    
    # Definición de columnas todas como STRING para la capa Workload (Bronze)
    columnas_sql = ', '.join([f'{field.name} STRING' for field in spark_schema.fields])
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{table_name} (
        {columnas_sql}
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    LINES TERMINATED BY '\\n'
    STORED AS TEXTFILE
    LOCATION '{location}'
    TBLPROPERTIES(
        'skip.header.line.count'='1'
    )
    """
    spark.sql(create_table_sql)
    
    spark.sql(f"INSERT OVERWRITE TABLE {db_name}.{table_name} SELECT * FROM tmp_{table_name}")
    print(f"✅ Tabla '{db_name}.{table_name}' desplegada en: {location}")

# =============================================================================
# @section 4. Definición de Esquema (Tus 21 columnas)
# =============================================================================

SCHEMAS = {
    "CUSTOMERS": StructType([
        StructField("customerID", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("SeniorCitizen", StringType(), True),
        StructField("Partner", StringType(), True),
        StructField("Dependents", StringType(), True),
        StructField("tenure", StringType(), True),
        StructField("PhoneService", StringType(), True),
        StructField("MultipleLines", StringType(), True),
        StructField("InternetService", StringType(), True),
        StructField("OnlineSecurity", StringType(), True),
        StructField("OnlineBackup", StringType(), True),
        StructField("DeviceProtection", StringType(), True),
        StructField("TechSupport", StringType(), True),
        StructField("StreamingTV", StringType(), True),
        StructField("StreamingMovies", StringType(), True),
        StructField("Contract", StringType(), True),
        StructField("PaperlessBilling", StringType(), True),
        StructField("PaymentMethod", StringType(), True),
        StructField("MonthlyCharges", StringType(), True),
        StructField("TotalCharges", StringType(), True),
        StructField("Churn", StringType(), True)
    ])
}

# =============================================================================
# @section 5. Proceso principal
# =============================================================================

def main():
    args = parse_arguments()
    spark = create_spark_session()
    
    try:
        # 1. Crear base de datos
        db_name = crear_database(spark, args.env, args.username, args.base_path)
        
        # 2. Configuración de tabla
        table_name = "CUSTOMERS"
        archivo_datos = "customers.data"
        esquema = SCHEMAS["CUSTOMERS"]
        
        # 3. Definición de rutas
        # archivo fuente ubicado en HDFS (no local)
        ruta_hdfs_datos = f"{args.data_path}/{archivo_datos}"
        # ubicación de salida en la base workload
        ruta_table_hive = f"{args.base_path}/{args.username}/datalake/{db_name}/{table_name.lower()}"
        
        print(f"📥 Procesando: {table_name} | Archivo HDFS Fuente: {ruta_hdfs_datos}")
        
        # 4. Lectura de datos desde HDFS
        df = spark.read.csv(
            ruta_hdfs_datos,
            schema=esquema,
            sep='|',
            header=True,
            nullValue='\\N',
            emptyValue=''
        )
        
        # 5. Crear tabla y cargar (usando la ruta de la tabla corregida)
        crear_tabla_external(spark, db_name, table_name, df, ruta_table_hive, esquema)
        
        print(f"🔍 Muestra de datos:")
        spark.sql(f"SELECT customerID, gender, Churn FROM {db_name}.{table_name} LIMIT 5").show()
        
        print("\n🎉 Proceso Workload completado!")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script PySpark para despliegue de capa Landing (AVRO + Particionamiento)
Proyecto: Telco Customer Churn - TOPICOSA
"""

import sys
import argparse
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# =============================================================================
# @section 1. Configuración de parámetros
# =============================================================================

def parse_arguments():
    parser = argparse.ArgumentParser(description='Proceso de carga - Capa Landing Churn')
    parser.add_argument('--env', type=str, default='topicosa', help='Entorno: topicosa, topicosb')
    parser.add_argument('--username', type=str, default='hadoop', help='Usuario HDFS')
    parser.add_argument('--base_path', type=str, default='/user', help='Ruta base en HDFS')
    parser.add_argument('--schema_path', type=str, default='/user/hadoop/datalake/schema', help='Ruta de esquemas AVRO')
    parser.add_argument('--source_db', type=str, default='workload', help='Base de datos origen')
    return parser.parse_args()

# =============================================================================
# @section 2. Inicialización de SparkSession
# =============================================================================

def create_spark_session(app_name="ProcesoLanding-TelcoChurn"):
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .config("spark.sql.avro.compression.codec", "snappy") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .getOrCreate()

# =============================================================================
# @section 3. Funciones auxiliares
# =============================================================================

def crear_database(spark, env, username, base_path):
    db_name = f"{env}_landing".lower()
    # Usamos Upper para la ruta HDFS como hace tu docente
    db_location = f"{base_path}/{username}/datalake/{db_name.upper()}"
    
    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{db_location}'")
    print(f"✅ Database '{db_name}' creada en: {db_location}")
    return db_name

def crear_tabla_avro_hive(spark, db_name, table_name, location, schema_avsc_url, partitioned_by=None):
    partition_clause = ""
    if partitioned_by:
        partition_cols = ", ".join([f"{c} STRING" for c in partitioned_by])
        partition_clause = f"PARTITIONED BY ({partition_cols})"
    
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
    {partition_clause}
    STORED AS AVRO
    LOCATION '{location}'
    TBLPROPERTIES (
        'avro.schema.url'='{schema_avsc_url}',
        'avro.output.codec'='snappy'
    )
    """
    spark.sql(create_sql)
    print(f"✅ Tabla AVRO '{db_name}.{table_name}' registrada en Hive Metastore")

def insertar_datos_avro(spark, db_name, table_name, df_source, partition_col=None):
    # Asegurar nombres de columnas en minúscula
    df_source = df_source.toDF(*[c.lower() for c in df_source.columns])
    
    table_full_name = f"{db_name}.{table_name}"
    
    print(f"🚀 Iniciando inserción en {table_full_name}...")
    
    if partition_col:
        p_col = partition_col.lower()
        # En Avro particionado, es vital que la columna de partición esté presente
        # Spark se encarga de acomodarla al escribir.
        df_source.write \
            .format("avro") \
            .mode("overwrite") \
            .partitionBy(p_col) \
            .saveAsTable(table_full_name)
    else:
        df_source.write \
            .format("avro") \
            .mode("overwrite") \
            .saveAsTable(table_full_name)
            
    # El comando mágico para que DBeaver vea los datos
    #spark.sql(f"MSCK REPAIR TABLE {table_full_name}")
    
    print(f"✅ Datos insertados y metadatos refrescados en '{table_full_name}'")

# =============================================================================
# @section 4. Configuración de tablas (Ajustado a tu proyecto)
# =============================================================================

TABLAS_CONFIG = [
    {
        "nombre": "CUSTOMERS",
        "archivo_avsc": "customers.avsc",
        "partitioned_by": None, # Evitamos la duplicidad con el esquema AVSC
        "dynamic_partition": True
    }
]

# =============================================================================
# @section 5. Proceso principal
# =============================================================================

def main():
    args = parse_arguments()
    spark = create_spark_session()
    
    try:
        env_lower = args.env.lower()
        db_landing = f"{env_lower}_landing"
        db_source = f"{env_lower}_workload"
        
        # 1. Crear database Landing
        crear_database(spark, env_lower, args.username, args.base_path)
        
        # 2. Procesar cada tabla
        for config in TABLAS_CONFIG:
            table_name = config["nombre"]
            print(f"📥 Procesando tabla Landing: {table_name}")
            
            # --- CAMBIO AQUÍ ---
            # Definimos la ubicación física de los datos de la tabla
            location = f"{args.base_path}/{args.username}/datalake/{db_landing.upper()}/{table_name.lower()}"
            
            # Ajustamos la URL del esquema para que apunte exactamente a la carpeta que creaste en HDFS
            # Agregamos 'hdfs://localhost:9000' (o solo hdfs://) para asegurar que Hive lo encuentre
            schema_url = f"hdfs://localhost:9000{args.schema_path}/{db_landing.upper()}/{config['archivo_avsc']}"
            # -------------------
            
            # A. Crear estructura AVRO
            crear_tabla_avro_hive(
                spark, db_landing, table_name, location, schema_url, config["partitioned_by"]
            )
            
            # B. Leer de Workload (Asegúrate que la tabla CUSTOMERS existe en topicosa_workload)
            df_source = spark.table(f"{db_source}.{table_name}")
            
            # C. Insertar
            insertar_datos_avro(
                spark, db_landing, table_name, df_source, 
                config["partitioned_by"][0] if config["partitioned_by"] else None
            )
            
            # D. Validación
            print(f"🔍 Validando carga en {db_landing}...")
            spark.sql(f"SELECT * FROM {db_landing}.{table_name} LIMIT 5").show()

        print("\n🎉 Proceso de Landing completado exitosamente!")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        # Esto nos ayudará a ver el error real en la consola
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()



#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script PySpark para despliegue de capa Curated (Silver) - Proyecto Telco Churn
Objetivo: Limpieza de datos (nulos, duplicados, tipos) y guardado en Parquet.
"""

import sys
import argparse
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim

# =============================================================================
# @section 1. Configuración de parámetros
# =============================================================================

def parse_arguments():
    parser = argparse.ArgumentParser(description='Proceso de limpieza - Capa Curated Churn')
    parser.add_argument('--env', type=str, default='topicosa', help='Entorno: topicosa, topicosb')
    parser.add_argument('--username', type=str, default='hadoop', help='Usuario HDFS')
    parser.add_argument('--base_path', type=str, default='/user', help='Ruta base en HDFS')
    return parser.parse_args()

# =============================================================================
# @section 2. Inicialización de SparkSession
# =============================================================================

def create_spark_session(app_name="Proceso_Curated_Churn"):
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .getOrCreate()

# =============================================================================
# @section 3. Funciones auxiliares
# =============================================================================

def crear_database(spark, env, username, base_path):
    db_name = f"{env}_curated".lower()
    # Usamos Upper para la ruta HDFS como en Landing
    db_location = f"{base_path}/{username}/datalake/{db_name.upper()}"
    
    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{db_location}'")
    print(f"✅ Database '{db_name}' creada en: {db_location}")
    return db_name, db_location

# =============================================================================
# @section 4. Proceso principal
# =============================================================================

def main():
    args = parse_arguments()
    spark = create_spark_session()
    
    try:
        env_lower = args.env.lower()
        db_curated = f"{env_lower}_curated"
        db_source = f"{env_lower}_landing"
        table_name = "customers"
        
        print(f"🚀 Iniciando procesamiento para la capa Curated (Silver)...")
        
        # 1. Crear database Curated
        db_name, db_location = crear_database(spark, env_lower, args.username, args.base_path)
        
        # 2. Leer datos de la capa Landing (Formato AVRO)
        print(f"📥 Leyendo datos crudos desde: {db_source}.{table_name}...")
        df_landing = spark.table(f"{db_source}.{table_name.lower()}")
        
        # ====================================================================
        # 3. APLICAR REGLAS DE CALIDAD (Requerimiento del Capítulo 5)
        # ====================================================================
        print(f"🧹 Aplicando limpieza de datos (Nulos, Duplicados y Tipos)...")
        
        # A. Eliminar duplicados completos
        df_limpio = df_landing.dropDuplicates()
        
        # B. Manejo de Nulos y Casteo (El problema de TotalCharges)
        # Los clientes con tenure=0 tienen espacios vacíos en TotalCharges. Los pasamos a 0.0
        df_limpio = df_limpio.withColumn("totalcharges", trim(col("totalcharges")))
        df_limpio = df_limpio.withColumn("totalcharges", 
                                         when(col("totalcharges") == "", "0.0")
                                         .otherwise(col("totalcharges")).cast("double"))
        
        # Castear MonthlyCharges a double por seguridad
        df_limpio = df_limpio.withColumn("monthlycharges", col("monthlycharges").cast("double"))
        
        # C. Castear SeniorCitizen de String/Int a Integer (0 o 1)
        df_limpio = df_limpio.withColumn("seniorcitizen", col("seniorcitizen").cast("int"))
        
        # D. Eliminar registros donde el ID del cliente sea nulo (Integridad)
        df_limpio = df_limpio.filter(col("customerid").isNotNull())
        
        # ====================================================================
        
        # 4. Guardar en formato PARQUET (Estándar para capa Silver/Curated)
        location_tabla = f"{db_location}/{table_name.lower()}"
        print(f"💾 Guardando datos limpios en formato Parquet en: {location_tabla}")
        
        # Guardamos particionando por el contrato (igual que en landing)
        df_limpio.write \
            .format("parquet") \
            .mode("overwrite") \
            .partitionBy("contract") \
            .option("path", location_tabla) \
            .saveAsTable(f"{db_curated}.{table_name}")
            
        print(f"✅ Tabla '{db_curated}.{table_name}' registrada exitosamente.")
        
        # 5. Muestra de validación
        print(f"🔍 Validando carga y limpieza en {db_curated}:")
        spark.sql(f"SELECT customerid, monthlycharges, totalcharges, seniorcitizen FROM {db_curated}.{table_name} LIMIT 5").show()
        
        print("\n🎉 Proceso de Curated completado exitosamente!")
        
    except Exception as e:
        print(f"❌ Error durante el proceso: {str(e)}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
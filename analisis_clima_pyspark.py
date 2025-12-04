"""
Proyecto 2 - Análisis de Datos Climáticos con PySpark
Dataset: NOAA Climate Data (GHCN-Daily)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, stddev, count, sum, desc, col, when
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from pathlib import Path

# Importar configuración
from config import (
    DATOS_PROCESADOS,
    RESULTADOS_DIR,
    SPARK_CONFIG,
    GRAFICAS_CONFIG
)
from utils import imprimir_banner, limpiar_archivos_temporales

# Configurar estilo de gráficas
plt.style.use('seaborn-v0_8-darkgrid')


def inicializar_spark():
    """Inicializa y configura Spark Session"""
    print("Inicializando Apache Spark...")
    
    # Crear builder de Spark
    builder = SparkSession.builder
    builder = builder.appName(SPARK_CONFIG['app_name'])
    builder = builder.config("spark.driver.memory", SPARK_CONFIG['driver_memory'])
    builder = builder.config("spark.sql.shuffle.partitions", "10")
    spark = builder.getOrCreate()
    
    # Reducir verbosidad de logs
    spark.sparkContext.setLogLevel(SPARK_CONFIG['log_level'])
    
    print(f"Spark {spark.version} iniciado")
    print(f"   App: {SPARK_CONFIG['app_name']}")
    print(f"   Memoria: {SPARK_CONFIG['driver_memory']}")
    
    return spark


def cargar_datos(spark, filepath):
    """
    Carga datos desde CSV a PySpark DataFrame
    
    Args:
        spark: SparkSession
        filepath: Path del archivo CSV
        
    Returns:
        DataFrame de PySpark
    """
    imprimir_banner("CARGA DE DATOS")
    
    print(f"\nArchivo: {filepath}")
    
    print("\n>>> Comando PySpark :")
    print(f"df = spark.read.csv('{filepath}', header=True, inferSchema=True)")
    
    # Leer CSV
    df = spark.read.csv(str(filepath), header=True, inferSchema=True)
    
    print("\n>>> df.count()")
    print(f"\nInformación del dataset:")
    print(f"   - Total registros: {df.count():,}")
    print(f"   - Columnas: {len(df.columns)}")
    
    print("\n>>> df.printSchema()")
    print("\nEsquema de datos:")
    df.printSchema()
    
    print("\n>>> df.show(5, truncate=False)")
    print("\nMuestra de datos:")
    df.show(5, truncate=False)
    
    return df


def procesamiento_1_temperatura_mensual(df):
    """
    PROCESAMIENTO 1: Temperatura Promedio Mensual
    Calcula estadísticas mensuales de temperatura por estación
    """
    imprimir_banner("PROCESAMIENTO 1: TEMPERATURA MENSUAL")
    
    print("\n>>> temp_mensual = df.groupBy('STATION', 'YEAR', 'MONTH') \\")
    print("    .agg(")
    print("        avg('TEMP').alias('temp_promedio'),")
    print("        max('TEMP').alias('temp_maxima'),")
    print("        min('TEMP').alias('temp_minima'),")
    print("        stddev('TEMP').alias('desv_std'),")
    print("        count('*').alias('num_registros')")
    print("    ) \\")
    print("    .orderBy('YEAR', 'MONTH')")
    print("\n>>> temp_mensual.show(15)")
    
    # Agregación con PySpark
    temp_mensual = df.groupBy("STATION", "YEAR", "MONTH") \
        .agg(
            avg("TEMP").alias("temp_promedio"),
            max("TEMP").alias("temp_maxima"),
            min("TEMP").alias("temp_minima"),
            stddev("TEMP").alias("desv_std"),
            count("*").alias("num_registros")
        ) \
        .orderBy("YEAR", "MONTH")
    
    print("\nResultados:")
    temp_mensual.show(15)
    
    # Convertir a Pandas solo los datos agregados (pequeños)
    temp_pandas = temp_mensual.toPandas()
    
    # Crear índice temporal para graficar
    temp_pandas['periodo'] = temp_pandas['YEAR'].astype(str) + '-' + temp_pandas['MONTH'].astype(str).str.zfill(2)
    
    # Graficar
    plt.figure(figsize=GRAFICAS_CONFIG['figsize_default'])
    
    # Tomar solo primeras estaciones para legibilidad
    estaciones_top = temp_pandas['STATION'].unique()[:3]
    
    for estacion in estaciones_top:
        data = temp_pandas[temp_pandas['STATION'] == estacion].head(50)
        plt.plot(range(len(data)), data['temp_promedio'], 
                marker='o', linewidth=2, markersize=3, label=estacion, alpha=0.7)
    
    plt.title('Temperatura Promedio Mensual por Estación', fontsize=14, fontweight='bold')
    plt.xlabel('Período (meses)')
    plt.ylabel('Temperatura (°C)')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    output_path = RESULTADOS_DIR / 'grafica_1_temp_mensual.png'
    plt.savefig(output_path, dpi=GRAFICAS_CONFIG['dpi'])
    plt.close()
    
    print(f"\nGráfica guardada: {output_path}")
    
    return temp_mensual


def procesamiento_2_precipitacion_anual(df):
    """
    PROCESAMIENTO 2: Precipitación Anual
    Calcula estadísticas anuales de precipitación
    """
    imprimir_banner("PROCESAMIENTO 2: PRECIPITACIÓN ANUAL")
    
    print("\n>>> precip_anual = df.groupBy('YEAR') \\")
    print("    .agg(")
    print("        sum('PRCP').alias('precip_total'),")
    print("        avg('PRCP').alias('precip_promedio'),")
    print("        stddev('PRCP').alias('desviacion_std'),")
    print("        max('PRCP').alias('precip_maxima'),")
    print("        count('*').alias('num_registros')")
    print("    ) \\")
    print("    .orderBy('YEAR')")
    print("\n>>> precip_anual.show()")
    
    # Agregación con PySpark
    precip_anual = df.groupBy("YEAR") \
        .agg(
            sum("PRCP").alias("precip_total"),
            avg("PRCP").alias("precip_promedio"),
            stddev("PRCP").alias("desviacion_std"),
            max("PRCP").alias("precip_maxima"),
            count("*").alias("num_registros")
        ) \
        .orderBy("YEAR")
    
    print("\nResultados:")
    precip_anual.show()
    
    # Convertir a Pandas
    precip_pandas = precip_anual.toPandas()
    
    # Crear figura con dos subgráficas
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=GRAFICAS_CONFIG['figsize_large'])
    
    # Gráfica 1: Precipitación total anual
    ax1.bar(precip_pandas['YEAR'], precip_pandas['precip_total'], 
            color='steelblue', alpha=0.7, edgecolor='black')
    ax1.set_title('Precipitación Total Anual', fontsize=14, fontweight='bold')
    ax1.set_ylabel('Precipitación Total (mm)')
    ax1.grid(True, alpha=0.3, axis='y')
    
    # Gráfica 2: Desviación estándar
    ax2.plot(precip_pandas['YEAR'], precip_pandas['desviacion_std'], 
             marker='s', color='coral', linewidth=2, markersize=6)
    ax2.set_title('Variabilidad de Precipitación (Desviación Estándar)', 
                  fontsize=14, fontweight='bold')
    ax2.set_xlabel('Año')
    ax2.set_ylabel('Desviación Estándar (mm)')
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    output_path = RESULTADOS_DIR / 'grafica_2_precipitacion.png'
    plt.savefig(output_path, dpi=GRAFICAS_CONFIG['dpi'])
    plt.close()
    
    print(f"\nGráfica guardada: {output_path}")
    
    return precip_anual


def procesamiento_3_extremos_climaticos(df):
    """
    PROCESAMIENTO 3: Extremos Climáticos por Estación
    Identifica récords de temperatura y precipitación
    """
    imprimir_banner("PROCESAMIENTO 3: EXTREMOS CLIMÁTICOS")
    
    print("\n>>> extremos = df.groupBy('STATION') \\")
    print("    .agg(")
    print("        max('TEMP').alias('temp_record_max'),")
    print("        min('TEMP').alias('temp_record_min'),")
    print("        max('PRCP').alias('precip_record'),")
    print("        avg('TEMP').alias('temp_media'),")
    print("        count('*').alias('num_observaciones')")
    print("    ) \\")
    print("    .orderBy(desc('temp_record_max'))")
    print("\n>>> extremos.show(10, truncate=False)")
    
    # Agregación con PySpark
    extremos = df.groupBy("STATION") \
        .agg(
            max("TEMP").alias("temp_record_max"),
            min("TEMP").alias("temp_record_min"),
            max("PRCP").alias("precip_record"),
            avg("TEMP").alias("temp_media"),
            count("*").alias("num_observaciones")
        ) \
        .orderBy(desc("temp_record_max"))
    
    print("\nResultados (Top 10 estaciones):")
    extremos.show(10, truncate=False)
    
    # Convertir a Pandas
    extremos_pandas = extremos.toPandas().head(10)
    
    # Graficar temperaturas extremas
    x = np.arange(len(extremos_pandas))
    width = 0.35
    
    fig, ax = plt.subplots(figsize=GRAFICAS_CONFIG['figsize_default'])
    
    bars1 = ax.bar(x - width/2, extremos_pandas['temp_record_max'], width, 
                   label='Temp. Máxima', color='red', alpha=0.7, edgecolor='black')
    bars2 = ax.bar(x + width/2, extremos_pandas['temp_record_min'], width, 
                   label='Temp. Mínima', color='blue', alpha=0.7, edgecolor='black')
    
    ax.set_xlabel('Estación Meteorológica')
    ax.set_ylabel('Temperatura (°C)')
    ax.set_title('Temperaturas Extremas por Estación', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels([f'Est-{i+1}' for i in range(len(extremos_pandas))], rotation=45)
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    ax.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    
    plt.tight_layout()
    output_path = RESULTADOS_DIR / 'grafica_3_extremos.png'
    plt.savefig(output_path, dpi=GRAFICAS_CONFIG['dpi'])
    plt.close()
    
    print(f"\nGráfica guardada: {output_path}")
    
    return extremos


def procesamiento_4_analisis_estacional(df):
    """
    PROCESAMIENTO 4: Análisis Estacional
    Compara variables climáticas por estación del año
    """
    imprimir_banner("PROCESAMIENTO 4: ANÁLISIS ESTACIONAL")
    
    print("\n>>> df_season = df.withColumn('SEASON',")
    print("    when((col('MONTH') >= 3) & (col('MONTH') <= 5), 'Primavera')")
    print("    .when((col('MONTH') >= 6) & (col('MONTH') <= 8), 'Verano')")
    print("    .when((col('MONTH') >= 9) & (col('MONTH') <= 11), 'Otoño')")
    print("    .otherwise('Invierno')")
    print(")")
    print("\nestacional = df_season.groupBy('SEASON') \\")
    print("    .agg(")
    print("        avg('TEMP').alias('temp_promedio'),")
    print("        avg('PRCP').alias('precip_promedio'),")
    print("        max('TEMP').alias('temp_maxima'),")
    print("        min('TEMP').alias('temp_minima'),")
    print("        count('*').alias('num_observaciones')")
    print("    )")
    print("\n>>> estacional.show()")
    
    # Definir estaciones del año con PySpark
    df_season = df.withColumn("SEASON", 
        when((col("MONTH") >= 3) & (col("MONTH") <= 5), "Primavera")
        .when((col("MONTH") >= 6) & (col("MONTH") <= 8), "Verano")
        .when((col("MONTH") >= 9) & (col("MONTH") <= 11), "Otoño")
        .otherwise("Invierno")
    )
    
    # Agregación
    estacional = df_season.groupBy("SEASON") \
        .agg(
            avg("TEMP").alias("temp_promedio"),
            avg("PRCP").alias("precip_promedio"),
            max("TEMP").alias("temp_maxima"),
            min("TEMP").alias("temp_minima"),
            count("*").alias("num_observaciones")
        )
    
    print("\nResultados por estación del año:")
    estacional.show()
    
    # Convertir a Pandas
    estacional_pandas = estacional.toPandas()
    
    # Ordenar estaciones cronológicamente
    orden_estaciones = ['Primavera', 'Verano', 'Otoño', 'Invierno']
    estacional_pandas['SEASON'] = pd.Categorical(
        estacional_pandas['SEASON'], 
        categories=orden_estaciones, 
        ordered=True
    )
    estacional_pandas = estacional_pandas.sort_values('SEASON')
    
    # Crear figura con dos subgráficas
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=GRAFICAS_CONFIG['figsize_large'])
    
    colores = ['#90EE90', '#FFD700', '#FF8C00', '#4682B4']
    
    # Temperatura por estación
    bars1 = ax1.bar(estacional_pandas['SEASON'], estacional_pandas['temp_promedio'], 
                    color=colores, alpha=0.7, edgecolor='black')
    ax1.set_title('Temperatura Promedio\npor Estación del Año', fontsize=13, fontweight='bold')
    ax1.set_ylabel('Temperatura (°C)')
    ax1.grid(True, alpha=0.3, axis='y')
    ax1.tick_params(axis='x', rotation=15)
    
    # Añadir valores en las barras
    for bar in bars1:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.1f}C', ha='center', va='bottom', fontsize=10)
    
    # Precipitación por estación
    bars2 = ax2.bar(estacional_pandas['SEASON'], estacional_pandas['precip_promedio'], 
                    color=colores, alpha=0.7, edgecolor='black')
    ax2.set_title('Precipitación Promedio\npor Estación del Año', fontsize=13, fontweight='bold')
    ax2.set_ylabel('Precipitación (mm/día)')
    ax2.grid(True, alpha=0.3, axis='y')
    ax2.tick_params(axis='x', rotation=15)
    
    # Añadir valores en las barras
    for bar in bars2:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.2f}mm', ha='center', va='bottom', fontsize=10)
    
    plt.tight_layout()
    output_path = RESULTADOS_DIR / 'grafica_4_estacional.png'
    plt.savefig(output_path, dpi=GRAFICAS_CONFIG['dpi'])
    plt.close()
    
    print(f"\nGráfica guardada: {output_path}")
    
    return estacional


def procesamiento_5_tendencia_correlacion(df):
    """
    PROCESAMIENTO 5: Tendencia Temporal y Correlación
    Analiza evolución temporal y relación entre variables
    """
    imprimir_banner("PROCESAMIENTO 5: TENDENCIAS Y CORRELACIÓN")
    
    print("\n>>> tendencia = df.groupBy('YEAR') \\")
    print("    .agg(")
    print("        avg('TEMP').alias('temp_anual'),")
    print("        avg('PRCP').alias('precip_anual'),")
    print("        count('*').alias('num_registros')")
    print("    ) \\")
    print("    .orderBy('YEAR')")
    print("\n>>> tendencia.show()")
    
    # Tendencia anual
    tendencia = df.groupBy("YEAR") \
        .agg(
            avg("TEMP").alias("temp_anual"),
            avg("PRCP").alias("precip_anual"),
            count("*").alias("num_registros")
        ) \
        .orderBy("YEAR")
    
    print("\nTendencia anual:")
    tendencia.show()
    
    print("\n>>> correlacion = df.stat.corr('TEMP', 'PRCP')")
    
    # Calcular correlación con PySpark
    correlacion = df.stat.corr("TEMP", "PRCP")
    print(f"\nCoeficiente de Correlación Temperatura-Precipitación: {correlacion:.4f}")
    
    # Interpretación - usar abs() de Python, no de PySpark
    import builtins
    if builtins.abs(correlacion) < 0.3:
        interpretacion = "débil"
    elif builtins.abs(correlacion) < 0.7:
        interpretacion = "moderada"
    else:
        interpretacion = "fuerte"
    
    print(f"   Interpretación: Correlación {interpretacion}")
    
    # Convertir a Pandas
    tendencia_pandas = tendencia.toPandas()
    
    # Crear gráfica de doble eje Y
    fig, ax1 = plt.subplots(figsize=GRAFICAS_CONFIG['figsize_default'])
    
    color1 = 'tab:red'
    ax1.set_xlabel('Año', fontsize=12)
    ax1.set_ylabel('Temperatura Promedio (°C)', color=color1, fontsize=12)
    line1 = ax1.plot(tendencia_pandas['YEAR'], tendencia_pandas['temp_anual'], 
                     color=color1, marker='o', linewidth=2.5, markersize=6, 
                     label='Temperatura', alpha=0.8)
    ax1.tick_params(axis='y', labelcolor=color1)
    ax1.grid(True, alpha=0.3)
    
    # Segundo eje Y
    ax2 = ax1.twinx()
    color2 = 'tab:blue'
    ax2.set_ylabel('Precipitación Promedio (mm/día)', color=color2, fontsize=12)
    line2 = ax2.plot(tendencia_pandas['YEAR'], tendencia_pandas['precip_anual'], 
                     color=color2, marker='s', linewidth=2.5, markersize=6, 
                     label='Precipitación', alpha=0.8)
    ax2.tick_params(axis='y', labelcolor=color2)
    
    # Título con correlación
    plt.title(f'Tendencia Temporal: Temperatura vs Precipitación\n(Correlación: {correlacion:.3f})', 
              fontsize=14, fontweight='bold', pad=20)
    
    # Leyenda combinada
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc='upper left')
    
    fig.tight_layout()
    output_path = RESULTADOS_DIR / 'grafica_5_tendencia.png'
    plt.savefig(output_path, dpi=GRAFICAS_CONFIG['dpi'])
    plt.close()
    
    print(f"\nGráfica guardada: {output_path}")
    
    return tendencia, correlacion


def main():
    """Función principal"""
    print("\n" + "="*60)
    print("PROYECTO 2 - ANÁLISIS CLIMÁTICO CON PYSPARK".center(60))
    print("="*60 + "\n")
    
    try:
        # Verificar que existen los datos
        if not DATOS_PROCESADOS.exists():
            print(f"Archivo no encontrado: {DATOS_PROCESADOS}")
            print("\nPrimero ejecuta: python descargar_datos_noaa.py")
            return
        
        # 1. Inicializar Spark
        spark = inicializar_spark()
        
        # 2. Cargar datos
        df = cargar_datos(spark, DATOS_PROCESADOS)
        
        # 3. Ejecutar los 5 procesamientos
        print("\n" + "="*60)
        print("EJECUTANDO 5 PROCESAMIENTOS")
        print("="*60)
        
        resultado1 = procesamiento_1_temperatura_mensual(df)
        resultado2 = procesamiento_2_precipitacion_anual(df)
        resultado3 = procesamiento_3_extremos_climaticos(df)
        resultado4 = procesamiento_4_analisis_estacional(df)
        resultado5, correlacion = procesamiento_5_tendencia_correlacion(df)
        
        # 4. Resumen final
        imprimir_banner("ANÁLISIS COMPLETADO")
        
        print(f"\nResumen de resultados:")
        print(f"   - Total de registros procesados: {df.count():,}")
        print(f"   - Procesamientos completados: 5/5")
        print(f"   - Gráficas generadas: 5")
        print(f"   - Directorio de resultados: {RESULTADOS_DIR}")
        
        print(f"\n>>> Comando PySpark :")
        print("df.select(")
        print("    avg('TEMP').alias('temp_promedio'),")
        print("    max('TEMP').alias('temp_maxima'),")
        print("    min('TEMP').alias('temp_minima'),")
        print("    avg('PRCP').alias('precip_promedio')")
        print(").show()")
        
        print(f"\nEstadísticas generales:")
        df.select(
            avg("TEMP").alias("temp_promedio"),
            max("TEMP").alias("temp_maxima"),
            min("TEMP").alias("temp_minima"),
            avg("PRCP").alias("precip_promedio")
        ).show()
        
        # 5. Cerrar Spark
        spark.stop()
        print(f"\nSesión Spark finalizada")
        
        # 6. Limpiar archivos temporales
        limpiar_archivos_temporales()
        
    except Exception as e:
        print(f"\nError durante el análisis: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*60)
    print("FIN DEL ANÁLISIS".center(60))
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
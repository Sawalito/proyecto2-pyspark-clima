"""
Configuración del proyecto
"""

import os
from pathlib import Path

# Directorios del proyecto
BASE_DIR = Path(__file__).resolve().parent
DATOS_DIR = BASE_DIR / "datos"
RESULTADOS_DIR = BASE_DIR / "resultados"
DOCS_DIR = BASE_DIR / "docs"

# Crear directorios si no existen
DATOS_DIR.mkdir(exist_ok=True)
RESULTADOS_DIR.mkdir(exist_ok=True)
DOCS_DIR.mkdir(exist_ok=True)

# Archivos de datos
DATOS_CRUDOS = DATOS_DIR / "datos_clima_noaa.csv"
DATOS_PROCESADOS = DATOS_DIR / "datos_clima_noaa_procesado.csv"
MUESTRA_5PCT = DATOS_DIR / "muestra_5porciento.csv"

# Configuración de Spark
SPARK_CONFIG = {
    "app_name": "Analisis_Clima_NOAA",
    "driver_memory": "4g",
    "executor_memory": "4g",
    "log_level": "ERROR"  # Reducir verbosidad de logs
}

# Estaciones meteorológicas a descargar
ESTACIONES_NOAA = [
    "USW00094728",  # NYC Central Park
    "USW00023174",  # Los Angeles
    "USW00013874",  # Chicago O'Hare
    "USW00012960",  # Houston
    "USW00023234",  # Phoenix
    "USC00045933",  # Philadelphia
    "USW00013881",  # San Antonio
    "USW00023188",  # San Diego
    "USW00013960",  # Dallas
    "USW00012919",  # Austin
]

# URL base de NOAA
NOAA_BASE_URL = "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/"

# Configuración de gráficas
GRAFICAS_CONFIG = {
    "dpi": 300,
    "figsize_default": (12, 6),
    "figsize_large": (14, 8),
    "style": "seaborn-v0_8-darkgrid"
}

# Requisitos del proyecto
REQUISITOS = {
    "tamaño_minimo_gb": 0.5,
    "periodo_minimo_años": 1,
    "num_procesamientos": 5
}

print(f"✓ Configuración cargada desde: {BASE_DIR}")
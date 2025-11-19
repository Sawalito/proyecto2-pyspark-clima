"""
Funciones auxiliares para el proyecto
"""

import os
from pathlib import Path
import pandas as pd
from datetime import datetime


def verificar_tamaño_archivo(filepath, tamaño_min_gb=0.5):
    """
    Verifica que un archivo cumpla el tamaño mínimo
    
    Args:
        filepath: Ruta al archivo
        tamaño_min_gb: Tamaño mínimo en GB
        
    Returns:
        bool: True si cumple, False si no
    """
    if not os.path.exists(filepath):
        print(f"❌ Archivo no encontrado: {filepath}")
        return False
    
    tamaño_gb = os.path.getsize(filepath) / (1024**3)
    print(f"📊 Tamaño del archivo: {tamaño_gb:.3f} GB")
    
    if tamaño_gb >= tamaño_min_gb:
        print(f"✅ Cumple requisito mínimo ({tamaño_min_gb} GB)")
        return True
    else:
        print(f"⚠️  No cumple requisito mínimo ({tamaño_min_gb} GB)")
        return False


def generar_muestra(filepath_entrada, filepath_salida, porcentaje=0.05, seed=42):
    """
    Genera una muestra aleatoria de un archivo CSV
    
    Args:
        filepath_entrada: Archivo CSV completo
        filepath_salida: Donde guardar la muestra
        porcentaje: Porcentaje de datos a muestrear (0.05 = 5%)
        seed: Semilla para reproducibilidad
    """
    print(f"\n📝 Generando muestra del {porcentaje*100}%...")
    
    # Leer con chunks para archivos grandes
    chunk_size = 100000
    sample_data = []
    
    for chunk in pd.read_csv(filepath_entrada, chunksize=chunk_size):
        sample_chunk = chunk.sample(frac=porcentaje, random_state=seed)
        sample_data.append(sample_chunk)
    
    # Combinar todos los chunks muestreados
    muestra = pd.concat(sample_data, ignore_index=True)
    muestra.to_csv(filepath_salida, index=False)
    
    tamaño_mb = os.path.getsize(filepath_salida) / (1024**2)
    print(f"✅ Muestra guardada: {filepath_salida}")
    print(f"   - Registros: {len(muestra):,}")
    print(f"   - Tamaño: {tamaño_mb:.2f} MB")


def imprimir_banner(texto):
    """Imprime un banner decorativo"""
    ancho = 60
    print("\n" + "=" * ancho)
    print(texto.center(ancho))
    print("=" * ancho)


def guardar_estadisticas(stats_dict, filepath):
    """
    Guarda estadísticas en un archivo de texto
    
    Args:
        stats_dict: Diccionario con estadísticas
        filepath: Ruta del archivo de salida
    """
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(f"ESTADÍSTICAS DEL PROYECTO\n")
        f.write(f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 60 + "\n\n")
        
        for key, value in stats_dict.items():
            f.write(f"{key}: {value}\n")
    
    print(f"✅ Estadísticas guardadas en: {filepath}")


def listar_archivos_datos(directorio):
    """Lista todos los archivos CSV en un directorio"""
    archivos = list(Path(directorio).glob("*.csv"))
    
    if not archivos:
        print(f"⚠️  No se encontraron archivos CSV en {directorio}")
        return []
    
    print(f"\n📁 Archivos encontrados en {directorio}:")
    total_size = 0
    
    for archivo in archivos:
        tamaño_mb = archivo.stat().st_size / (1024**2)
        total_size += tamaño_mb
        print(f"   - {archivo.name} ({tamaño_mb:.2f} MB)")
    
    print(f"\n📊 Total: {len(archivos)} archivos, {total_size:.2f} MB")
    return archivos


def verificar_dependencias():
    """Verifica que todas las dependencias estén instaladas"""
    dependencias = ['pyspark', 'pandas', 'matplotlib', 'requests']
    
    print("\n🔍 Verificando dependencias...")
    todas_ok = True
    
    for dep in dependencias:
        try:
            __import__(dep)
            print(f"   ✅ {dep}")
        except ImportError:
            print(f"   ❌ {dep} - NO INSTALADO")
            todas_ok = False
    
    if todas_ok:
        print("\n✅ Todas las dependencias están instaladas")
    else:
        print("\n❌ Faltan dependencias. Ejecuta: pip install -r requirements.txt")
    
    return todas_ok


def limpiar_archivos_temporales(directorio="."):
    """Elimina archivos temporales de Spark"""
    archivos_temp = [
        "derby.log",
        "metastore_db",
        "spark-warehouse"
    ]
    
    for item in archivos_temp:
        path = Path(directorio) / item
        if path.exists():
            if path.is_file():
                path.unlink()
                print(f"🗑️  Eliminado: {item}")
            elif path.is_dir():
                import shutil
                shutil.rmtree(path)
                print(f"🗑️  Eliminado directorio: {item}")


if __name__ == "__main__":
    # Test de funciones
    print("🧪 Probando funciones auxiliares...")
    verificar_dependencias()
    imprimir_banner("PROYECTO 2 - PYSPARK")
    print("\n✅ Módulo utils.py funcionando correctamente")
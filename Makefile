# Makefile para Proyecto 2 - PySpark

.PHONY: help install download analyze clean docker-build docker-run test

help:
	@echo "Comandos disponibles:"
	@echo "  make install       - Instalar dependencias"
	@echo "  make download      - Descargar datos de NOAA"
	@echo "  make analyze       - Ejecutar análisis PySpark"
	@echo "  make all           - Ejecutar todo el pipeline"
	@echo "  make clean         - Limpiar archivos temporales"
	@echo "  make docker-build  - Construir imagen Docker"
	@echo "  make docker-run    - Ejecutar en Docker"
	@echo "  make test          - Verificar instalación"
	@echo "  make muestra       - Generar muestra 5%"

install:
	pip install -r requirements.txt

download:
	python descargar_datos_noaa.py

analyze:
	python analisis_clima_pyspark.py

all: download analyze

clean:
	rm -f *.log
	rm -rf metastore_db spark-warehouse
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

docker-build:
	docker-compose build

docker-run:
	docker-compose run pyspark-app

test:
	python utils.py
	python -c "import pyspark; print('PySpark OK')"
	python -c "import pandas; print('Pandas OK')"
	python -c "import matplotlib; print('Matplotlib OK')"

muestra:
	python -c "from utils import generar_muestra; from config import DATOS_PROCESADOS, MUESTRA_5PCT; generar_muestra(DATOS_PROCESADOS, MUESTRA_5PCT)"
	
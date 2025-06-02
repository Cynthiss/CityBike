# 🚲 CityBike Data Engineering

Un proyecto de ingeniería de datos de extremo a extremo basado en los datos abiertos de Citi Bike (NYC), que transforma más de 10 años de información cruda en métricas útiles para el análisis de movilidad urbana.

---

## 📌 Descripción general

Este proyecto tiene como objetivo construir un pipeline automatizado para la ingestión, limpieza, transformación, validación y visualización de datos de viajes en bicicleta, provenientes del bucket público de Amazon S3:  
🔗 https://s3.amazonaws.com/tripdata/index.html

Utilizamos el enfoque **Medallion Architecture** (Bronze – Silver – Gold) junto con herramientas modernas como **Pandas, Prefect, Great Expectations y Power BI**.

---

## 🎯 Objetivos

### Objetivos generales:
- Proveer una solución reproducible y escalable de análisis de datos de transporte.
- Automatizar el procesamiento de datos crudos hasta dashboards analíticos.

### Objetivos específicos:
- Procesar todos los archivos CSV del bucket S3 (2013–2024).
- Implementar una arquitectura modular para limpieza y agregación.
- Validar los datos con reglas de calidad definidas.
- Visualizar métricas clave como duración media, estaciones más utilizadas, y horas pico.

---

## 🧰 Herramientas utilizadas

| Etapa                    | Herramienta                  |
|--------------------------|------------------------------|
| Descarga                 | `requests`, `BeautifulSoup` |
| Extracción de archivos   | `zipfile`                    |
| Procesamiento de datos   | `pandas`, `pyarrow`          |
| Almacenamiento           | `.parquet`                   |
| Orquestación             | `Prefect`                    |
| Validación de calidad    | `Great Expectations`         |
| Visualización            | `Power BI`                   |
| Diagrama de arquitectura | `draw.io`                    |
| Control de versiones     | `GitHub`                     |

---

## 🧱 Arquitectura del pipeline


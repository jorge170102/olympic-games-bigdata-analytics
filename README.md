# Análisis de datos distribuidos — Big Data MCDI502

**Magíster en Ciencia de Datos e Inteligencia Artificial · Universidad Andrés Bello (UNAB)**  
**Grupo 7 · Cristian Grandón, Felipe Palma y Jorge Gutiérrez**

Este repositorio contiene los talleres prácticos desarrollados con Python, Apache Spark, PySpark y Spark SQL. Los notebooks están separados por evaluación para facilitar su lectura, ejecución y entrega en Canvas.

| Evaluación | Notebook | Plataforma exigida por la pauta | Entrega | Ponderación |
|---|---|---|---|---|
| Sumativa 2 · Taller I | `mcdi502_s2_grupo7.ipynb` | Notebook ejecutable (VS Code/Jupyter o Colab) | Notebook + video, ZIP | 40% |
| Sumativa 3 · Taller II | [`mcdi502_s3_grupo7.ipynb`](mcdi502_s3_grupo7.ipynb) | Notebook ejecutable (Colab sugerido; no exclusivo) | Notebook + video, ZIP | 20% |
| Sumativa 4 · Taller III | [`mcdi502_s4_grupo7.ipynb`](mcdi502_s4_grupo7.ipynb) | **Databricks obligatorio** | Notebook Databricks, ZIP | 20% |

> **Estado de las versiones actualizadas:** los notebooks S3 y S4 pasaron validación estructural (`nbformat`) y análisis de sintaxis, **pero sus conexiones y consultas deben ejecutarse en los entornos del grupo**. Las credenciales de MongoDB, el token AQICN, los servicios externos y la sesión de Databricks no están disponibles en este repositorio. No se incluyen resultados simulados ni se afirma haber realizado una ejecución remota.

## Estructura sugerida

```text
olympic-games-bigdata-analytics/
├── data/
│   ├── deportista.csv
│   ├── deportista2.csv
│   ├── equipo.csv
│   ├── evento.csv
│   ├── juegos.json
│   ├── resultados.csv
│   ├── ventas_concepcion.csv
│   ├── ventas_santiago.csv
│   └── ventas_vina_del_mar.csv
├── src/
│   ├── __init__.py
│   └── olimpicos.py
├── mcdi502_s2_grupo7.ipynb
├── mcdi502_s3_grupo7.ipynb
├── mcdi502_s4_grupo7.ipynb
├── requirements.txt
├── .gitignore
└── README.md
```

Los archivos originales proporcionados por la asignatura se conservan sin modificaciones. **No se suben** contraseñas, tokens, carpetas `.venv/`, bases privadas ni archivos temporales de sesiones.

---

## Sumativa 2 — Juegos Olímpicos

Se integran los registros de deportistas, equipos, eventos, juegos y resultados con RDDs y DataFrames de PySpark. Se implementan particionamiento, transformación, cálculo de IMC, medallas por equipo y estadísticas de altura/edad, además de una consulta Spark SQL.

- El módulo `src/olimpicos.py` agrupa funciones reutilizables que el notebook importa.
- En la ejecución previamente documentada de la Sumativa 2 se unieron **135.571 deportistas** (67.786 + 67.785), se preservaron **271.116 resultados** y se aplicaron **6 particiones al RDD inicial** y **5 particiones al DataFrame integrado**.
- Las estadísticas de deportistas y el conteo de medallas se hacen sobre registros históricos de participaciones; en deportes colectivos, una medalla de podio puede aparecer en varias filas.
- El entorno local necesita un JDK compatible, Python y PySpark. El archivo `requirements.txt` facilita la instalación de paquetes Python, **pero no instala Java**.
- La entrega oficial de la Sumativa 2 es el notebook más un video grupal en un único ZIP; si se mantiene la arquitectura modular, el ZIP necesita también los archivos `src/` y las fuentes `data/` para ser ejecutable.

## Sumativa 3 — Cuatro fuentes heterogéneas

**Objetivo:** crear cuatro DataFrames de **100 registros reales cada uno** y mostrar sus resultados, dos columnas transformadas, integración explicada y las consultas **C1 a C7**.

| Fuente | DataFrame | Acceso | Variables exigidas |
|---|---|---|---|
| Open-Meteo | `df_openmeteo` | API pública | Temperatura y humedad a 2 m, precipitación, viento 10 m, dirección del viento e índice UV |
| AQICN | `df_aqicn` | API con token personal | PM2.5, PM10, O3, NO2, SO2 |
| MongoDB ESP32 | `df_mongo_bpm` | URI y autorización del docente | Altura en pies, presión atmosférica, monóxido de carbono |
| MySQL ESP32 | `df_mysql_dht` | Endpoint JSON/REST del curso | Temperatura interior y humedad |

### Ejecutar en Google Colab

1. Abre `mcdi502_s3_grupo7.ipynb` desde Colab. La primera celda de código instala paquetes Python si faltan y comprueba Java; puede requerir internet.
2. Ten disponible un **token válido** de AQICN para la ciudad elegida y la **URI de MongoDB autorizada** por el docente. Se introducen de manera interactiva sin escribirlas en el repositorio.
3. Ejecuta el notebook **desde cero y en orden**, con la sesión limpia. La recolección de AQICN efectúa 100 solicitudes separadas por un segundo y puede estar limitada por las políticas de la API.
4. Confirma en la sección de control que **cada DataFrame tiene 100 registros** y que AQICN corresponde a la región solicitada.
5. Verifica la columna `altura_metros`, la clasificación `condicion_clima`, la integración `df_unificado` y las **siete consultas Spark SQL**.
6. Guarda el notebook con sus salidas. Graba con los tres integrantes un video de **7 a 10 minutos** y entrega ambos archivos dentro de un ZIP.

**Aspecto crítico:** AQICN puede devolver la misma medición en sucesivas consultas; el notebook distingue **100 capturas** de **100 mediciones históricas distintas**. La pauta exige 100 registros por fuente, pero la API no garantiza 100 mediciones diferentes al consultar una estación repetidamente. No consideres esto resuelto si el docente interpreta «registros» como observaciones independientes.

**Otro límite externo:** si MongoDB o MySQL contiene menos de 100 registros reales, el notebook presenta el número de filas disponible y marca el requisito como pendiente. Debes conseguir más datos o acceso autorizado por el docente; **no dupliques filas** para aparentar la cantidad.

### Ejecutar en VS Code/Jupyter (opcional para S3)

```powershell
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
java -version
```

Requiere Java JDK instalado por separado y un kernel que corresponda a `.venv`. El notebook S3 no necesita `src/` ni archivos CSV locales: lee las cuatro fuentes reales por API/BD. La instalación de Java en Colab, cuando sea necesaria, se realiza por comandos de la primera celda; en Windows Java se instala manualmente.

### Consultas de la pauta S3

- **C1:** diez temperaturas más altas, orden descendente.
- **C2:** promedio de humedad para un día y mes identificados.
- **C3:** cantidad de registros con precipitación sobre 2 mm en la muestra de la última semana.
- **C4:** promedios de PM2.5, PM10, O3, NO2 y SO2.
- **C5:** cantidad de registros con NO2 superior a 4.
- **C6:** diez registros MongoDB incluyendo `altura_metros`.
- **C7:** diez registros Open-Meteo incluyendo `condicion_clima`.

## Sumativa 4 — Ventas retail en Databricks

**Atención:** la guía exige **desarrollo íntegro en Databricks**, no una ejecución local que imite su entorno. El notebook nuevo está diseñado para importarse en ese servicio; utiliza las variables `spark` y `dbutils` provistas por Databricks y lee archivos desde DBFS o un Volume.

### Ejecutar y guardar

1. Descarga de Canvas los tres CSV originales: `ventas_vina_del_mar.csv`, `ventas_santiago.csv` y `ventas_concepcion.csv`.
2. Súbelos al **Databricks File System (DBFS)** del espacio de evaluación. Si tu cuenta usa Unity Catalog Volumes, carga allí los archivos y reemplaza `RUTA_BASE` por `/Volumes/<catalogo>/<esquema>/<volumen>`.
3. Abre e importa `mcdi502_s4_grupo7.ipynb` en Databricks y asígnale cómputo activo (un clúster o entorno compatible).
4. Ejecuta **Run all**. Verifica que cada DataFrame muestre esquema y cinco filas, que el conteo del unificado sea la suma de los tres CSV y que las conversiones funcionen.
5. Comprueba la eliminación de duplicados, imputación de categorías, normalización de nombres, fechas y precios, el filtrado `cantidad > 0`, `precio_total`, `dia_semana`, el IQR y el cuadro de calidad **antes/después**.
6. Verifica la vista temporal `ventas_limpias` y las **tres consultas `spark.sql()`**.
7. Guarda las salidas, **exporta el notebook** como `mcdi502_s4_grupo7.ipynb`, comprímelo en un solo ZIP y súbelo a Canvas. **No se solicita video para la Sumativa 4.**

### Validaciones explícitas

- Integridad: `registros_unificados == registros_vina + registros_santiago + registros_concepcion`.
- Reconciliación: `filas_originales - duplicados - rechazadas == filas_finales`.
- Datos válidos: ninguna cantidad es menor o igual a cero y ningún `precio_total` es negativo.
- Calidad: recuento de nulos/vacíos por columna y duplicados **antes y después**.
- Valores atípicos: regla IQR en `precio_unitario` con outliers marcados; no se eliminan en silencio.

Los resultados de ventas pueden variar si los datos docentes cambian. **No se codifican conteos ni importes fijos** para aparentar una ejecución.

---

## Uso de GitHub y entrega oficial

GitHub se utiliza para versionar y colaborar, **no sustituye Canvas**.

```bash
git add README.md requirements.txt mcdi502_s3_grupo7.ipynb mcdi502_s4_grupo7.ipynb
git commit -m "Mejora Sumativas 3 y 4 conforme a rúbricas"
git push origin main
```

No incluyas archivos `.venv/`, credenciales, tokens, ni información personal de las fuentes.

**Fecha límite indicada en las dos pautas:** domingo 15 de noviembre de 2026, 23:59 (hora local del curso). Consulta Canvas para verificar el estado efectivo de la entrega.

## Verificaciones de la actualización

- Los dos archivos `.ipynb` fueron validados con `nbformat` y sus celdas de código pasaron comprobaciones de sintaxis con `ast.parse`.
- **No se han ejecutado aquí** las conexiones remotas de AQICN, MongoDB, MySQL ni el workspace de Databricks. Las pruebas finales deben realizarse con el acceso real del grupo.
- Para evitar evidencias engañosas, los notebooks actualizados se entregan **sin salidas previas**. Antes de enviar a Canvas, ejecuta y guarda los resultados reales.

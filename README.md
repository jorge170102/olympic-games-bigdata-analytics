# Análisis histórico de los Juegos Olímpicos con Apache Spark

**Evaluación Sumativa 2 · Taller 1 · MCDI502 — Gestión de Datos y Tecnologías Big Data**  
**Magíster en Ciencia de Datos e Inteligencia Artificial · Universidad Andrés Bello · Grupo 7**

![Apache Spark](https://img.shields.io/badge/Apache_Spark-4.0.3-E25A1C?logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.14.8-3776AB?logo=python&logoColor=white)
![Jupyter](https://img.shields.io/badge/Jupyter-Notebook-F37626?logo=jupyter&logoColor=white)
![Estado](https://img.shields.io/badge/Estado-Evidencia_de_ejecuci%C3%B3n_disponible-2E7D32)

## 1. Descripción

Este proyecto analiza registros históricos de participaciones olímpicas mediante **Apache Spark y PySpark**. Integra las entidades de deportistas, resultados, equipos, eventos y ediciones de los juegos para aplicar operaciones de procesamiento distribuido: **RDD, DataFrames, transformaciones, uniones, particionamiento, funciones de agregación y Spark SQL**.

El procesamiento se realiza en un notebook Jupyter ejecutable localmente desde VS Code. Las funciones de lectura, normalización y análisis reutilizables se encuentran separadas en el módulo [`src/olimpicos.py`](src/olimpicos.py), mientras que el notebook [`Sumativa_2_grupo7.ipynb`](Sumativa_2_grupo7.ipynb) organiza el flujo analítico y presenta sus resultados.

## 2. Equipo de trabajo

**Grupo 7 — Universidad Andrés Bello**

- Felipe Palma Barrientos
- Vicente Aguila Rojas
- Cristian Grandon Grandon
- Jorge Gutierrez Jaramillo

## 3. Objetivos

1. Inicializar una sesión de Spark en un entorno local reproducible.
2. Crear, unir y transformar RDD; inspeccionar sus registros y particiones.
3. Construir DataFrames con esquemas y tipos de datos adecuados.
4. Integrar información de deportistas, resultados, equipos, eventos y juegos olímpicos.
5. Aplicar reparticionamiento y persistencia para ilustrar el procesamiento distribuido.
6. Calcular columnas derivadas y agregaciones descriptivas, además de consultar los datos mediante Spark SQL.
7. Validar la conservación de registros y documentar decisiones de calidad de datos.

## 4. Estructura del repositorio

```text
olympic-games-bigdata-analytics/
├── data/
│   ├── deportista.csv
│   ├── deportista2.csv
│   ├── equipo.csv
│   ├── evento.csv
│   ├── juegos.json
│   └── resultados.csv
├── src/
│   ├── __init__.py
│   └── olimpicos.py
├── Sumativa_2_grupo7.ipynb
├── requirements.txt
├── .gitignore
└── README.md
```

Los seis archivos de entrada están versionados en `data/`. La carpeta `data_procesada/` se crea al ejecutar el notebook y está excluida de Git mediante `.gitignore`; almacena la copia normalizada de eventos sin sobrescribir los datos originales.

### Funciones del módulo `src/olimpicos.py`

| Función | Propósito |
|---|---|
| `campos_deportista()` | Lee los siete campos de cada registro y admite una coma final sobrante. |
| `entero_o_none()` / `decimal_o_none()` | Convierte valores numéricos y trata marcadores de ausencia como `None`. |
| `parsear_deportista()` | Convierte cada registro a una tupla con tipos apropiados. |
| `preparar_eventos()` | Normaliza filas encapsuladas en comillas y escribe una copia procesada. |
| `calcular_columnas()` | Incorpora el índice de masa corporal (IMC) y la descripción del sexo. |
| `medallas_equipo()` | Cuenta registros con medalla de oro, plata y bronce por equipo. |
| `estadisticas()` | Calcula suma, cantidad válida, promedio, máximo y mínimo por grupo. |

## 5. Entorno y ejecución

**Entorno con ejecución documentada en el notebook:** Python **3.14.8**, PySpark / Spark **4.0.3** y **Java JDK 17**. El archivo [`requirements.txt`](requirements.txt) declara `pyspark==4.0.3` e `ipykernel>=6,<8`. En otros equipos, es necesario comprobar que la combinación instalada de Python, Java y PySpark sea compatible.

En **PowerShell**, desde la raíz de este repositorio:

```powershell
# Crear entorno virtual
py -m venv .venv

# Instalar dependencias utilizando el Python del entorno
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

# Comprobar disponibilidad del JDK
java -version
```

Abrir [`Sumativa_2_grupo7.ipynb`](Sumativa_2_grupo7.ipynb) en VS Code, seleccionar el kernel de `.venv` y elegir **Restart Kernel → Run All**. Iniciar Jupyter desde la **raíz del repositorio**: el notebook toma `Path.cwd()` como directorio base y espera encontrar allí `data/`.

Si `java -version` falla, instalar el JDK y revisar `JAVA_HOME` y `PATH` antes de reiniciar VS Code. Se recomienda volver a guardar el notebook después de ejecutar todas las celdas para conservar sus salidas y una numeración secuencial completa.

## 6. Datos y decisiones de calidad

| Fuente | Registros | Consideración aplicada |
|---|---:|---|
| `deportista.csv` | 67.786 | Lectura controlada de una fila con coma final sobrante (ID 16587) |
| `deportista2.csv` | 67.785 | Unión con el primer RDD; los pesos decimales se representan como `double` |
| `equipo.csv` | 1.184 | Catálogo de equipos para enriquecimiento de resultados |
| `evento.csv` | 765 | Normalización de 280 filas encapsuladas en comillas, sobre copia derivada |
| `resultados.csv` | 271.116 | Separador `;`; nueve `evento_id` no válidos se convierten en `NULL` |
| `juegos.json` | 51 | Ajuste de nombres: el campo original `temporada` contiene el año y `ciudad` la estación |

### Consideraciones de calidad

- La conversión `try_cast(evento_id AS INT)` evita que identificadores no válidos interrumpan la lectura; se detectan **9 resultados sin nombre de evento**.
- Se realizan uniones `left` a partir de `Resultado`, conservando sus **271.116 filas**. Se identifican **200 resultados sin equipo catalogado** y **0 sin deportista relacionado**.
- En las estadísticas de edad, altura y peso, el valor `0` se interpreta como no informado y no se utiliza como una medición válida.
- El IMC solo se calcula cuando peso y altura son positivos: `peso_kg / (altura_cm / 100)²`.
- Las estadísticas describen **registros de participaciones**, no necesariamente deportistas únicos ni relaciones causales.

### Aclaración sobre `temporada` y `anio`

El archivo original `juegos.json` utiliza el campo **`temporada` para almacenar el año de celebración** y el campo **`ciudad` para identificar la estación olímpica** (Verano o Invierno). Para mejorar la interpretación, el notebook renombra estos campos de la siguiente manera:

| Campo original (`juegos.json`) | Campo utilizado en el notebook | Interpretación |
|---|---|---|
| `temporada` | `anio` | Año de celebración de los Juegos Olímpicos. |
| `ciudad` | `temporada` | Estación olímpica: Verano o Invierno. |
| `ano` | `edicion` | Campo original conservado bajo un nombre diferenciado. |

**La agregación denominada `temporada` se agrupa por `anio`**, es decir, por el contenido del campo originalmente llamado `temporada`. Esta decisión conserva la agrupación solicitada sobre la variable de origen y evita confundir el año con la estación olímpica. El DataFrame resultante sigue llamándose `temporada`.

## 7. Metodología aplicada

| Etapa | Implementación en el notebook | Evidencia |
|---|---|---|
| Inicialización | `SparkSession`, `SparkContext` y lectura de archivos | Spark 4.0.3 y seis archivos detectados |
| RDD | Primer RDD con **6 particiones**, segundo RDD y `union()` | 67.786 + 67.785 = **135.571** registros |
| Transformaciones | `filter()` para mayores de edad y mujeres; `map()` a mayúsculas | Conteos y muestras visibles |
| DataFrame de deportistas | Conversión del RDD unido a un esquema tipado de **7 columnas** | `printSchema()` y ejemplos |
| Integración | DataFrames de Evento, Resultado, Equipos y Juego; `left joins` y `broadcast()` | **271.116** resultados preservados |
| Paralelismo | `repartition(5)` y persistencia en caché | **5 particiones** verificadas |
| Exploración | `count()`, `dtypes`, `printSchema()` y `show()` | Conteos, tipos y esquema |
| Columnas derivadas | IMC con altura expresada en metros y `Descripción_sexo` | Nuevas columnas comprobadas |
| Agregaciones | Medallas por equipo; edad por medalla y sexo; altura por temporada | Tablas de suma, promedio, máximo y mínimo |
| SQL | Vista temporal y `spark.sql()` sobre resultados olímpicos | Medallas por temporada y tipo |

### Agregaciones implementadas

1. **Medallas por equipo:** cantidad de registros con `Gold`, `Silver` y `Bronze`, además del total por equipo.
2. **Edad por tipo de medalla:** suma, promedio, máximo y mínimo, excluyendo edades no informadas.
3. **DataFrame `temporada`:** suma, promedio, máximo y mínimo de la altura en centímetros **por año de celebración** (`anio`).
4. **DataFrame `sexo`:** suma, promedio, máximo y mínimo de la edad según sexo, entre registros válidos.

La función general `estadisticas()` añade también `cantidad_deportistas`, que cuenta los registros con un valor válido de la variable analizada. Este nombre de columna no implica necesariamente personas únicas.

## 8. Resultados obtenidos

### Controles de procesamiento

| Indicador | Resultado |
|---|---:|
| Registros de deportistas unidos | **135.571** |
| Deportistas mayores de edad (registros del RDD) | **123.244** |
| Registros de mujeres (RDD) | **33.981** |
| Resultados olímpicos integrados | **271.116** |
| Particiones del RDD inicial | **6** |
| Particiones del DataFrame reparticionado | **5** |
| Registros con medalla (`Gold`, `Silver`, `Bronze`) | **39.783** |
| Referencias a eventos no resueltas | **9** |
| Referencias a equipos no resueltas | **200** |

Los controles de la última celda informan **«VALIDACIONES ESTRUCTURALES: OK»** y corroboran que la suma de registros con medalla agrupados por equipo coincide con el total de registros premiados.

### Ejemplos de análisis descriptivo

- **Registros con medalla por equipo:** Estados Unidos **5.310**, Unión Soviética **2.604** y Alemania **1.890**.
- **Edad media por tipo de medalla:** bronce **23,69**, oro **23,70** y plata **23,75** años, entre registros con edad válida.
- **Altura media por temporada:** Invierno **174,59 cm** y Verano **175,52 cm**, entre registros con altura válida.
- **Edad media por sexo:** hombre **24,43** y mujer **21,86** años, entre registros con edad válida.

**Interpretación importante:** el conteo de medallas suma **filas de deportistas con medalla**. No equivale necesariamente al número de podios olímpicos únicos, especialmente en deportes colectivos. Las estadísticas corresponden a los registros históricos presentes en este conjunto de datos y no implican causalidad.

## 9. Evidencia de ejecución y reproducibilidad

El notebook publicado incluye **18 celdas de código**, todas con salidas guardadas y **sin errores de ejecución registrados** en la versión auditada. Las salidas evidencian que Spark funcionó, aunque **no sustituyen una nueva ejecución integral desde un kernel limpio** antes de cerrar el proyecto.

Antes de usarlo como versión definitiva, ejecutar **Restart Kernel → Run All**, comprobar que las celdas se numeren secuencialmente y guardar el `.ipynb`. Se espera que continúen cumpliéndose, entre otros, estos controles:

```text
RDD inicial:                    6 particiones
RDD unido:                      135571 registros
DataFrame de resultados:        271116 registros
DataFrame reparticionado:       5 particiones
Registros con medalla:          39783
Verificaciones estructurales:   OK
```

La carpeta temporal `data_procesada/` puede regenerarse a partir de los seis archivos originales. No se requieren archivos auxiliares externos al árbol publicado.

## 10. Alcance

El repositorio documenta la **implementación técnica y sus resultados**. La evaluación académica final depende de la pauta oficial y de la comprobación del trabajo por el docente; los controles del código no constituyen una calificación ni una certificación de todos los puntos posibles.

---

**Grupo 7 · MCDI502 · Big Data · Universidad Andrés Bello**

# Sumativa 2 · Big Data con Apache Spark

**MCDI502 · Taller 1 · Universidad Andrés Bello (UNAB) · Grupo 7**

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-Data%20Processing-E25A1C?logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)
![Jupyter](https://img.shields.io/badge/Jupyter-Notebook-F37626?logo=jupyter&logoColor=white)
![VS Code](https://img.shields.io/badge/VS%20Code-Entorno%20local-007ACC?logo=visualstudiocode&logoColor=white)
![Estado](https://img.shields.io/badge/Estado-Validaci%C3%B3n%20Spark%20pendiente-yellow)

## Descripción del proyecto

Este repositorio reúne el desarrollo de la **Sumativa 2 de Big Data (Taller 1)**, centrada en el análisis de datos históricos de participaciones olímpicas mediante **Apache Spark y PySpark**. El propósito es aplicar procesamiento distribuido usando **RDD, DataFrames, transformaciones, particionamiento, joins y Spark SQL** para explorar características de deportistas, equipos, eventos, juegos y resultados.

El proyecto está organizado para trabajar **localmente con VS Code y Jupyter** y versionar el trabajo en GitHub. **No requiere Google Colab, MySQL ni PostgreSQL** para los procedimientos descritos.


## Equipo de trabajo

**Grupo 7 · Magíster en Ciencia de Datos e Inteligencia Artificial · UNAB**

- Felipe Palma Barrientos
- Vicente Aguila Rojas
- Cristian Grandon Grandon
- Jorge Gutierrez Jaramillo

## Objetivos

1. Configurar una sesión local de Apache Spark y comprender la relación entre `SparkSession` y `SparkContext`.
2. Procesar información olímpica con **RDD y DataFrames**, aplicando transformaciones y operaciones distribuidas.
3. Integrar archivos CSV y JSON mediante uniones y tratamientos de calidad de datos documentados.
4. Analizar variables derivadas, medallas y características demográficas a través de operaciones Spark y consultas SQL.
5. Presentar resultados reproducibles en un notebook y comunicarlos mediante un video grupal.

## Estructura del proyecto

```text
Sumativa2_BigData_UNAB/
├── data/
│   ├── deportista.csv          # 67.786 registros
│   ├── deportista2.csv         # 67.785 registros
│   ├── equipo.csv              # 1.184 equipos
│   ├── evento.csv              # 765 eventos
│   ├── resultados.csv          # 271.116 resultados
│   └── juegos.json             # 51 juegos
├── mcdi502_s2_grupox.ipynb     # Notebook principal; nombre por ajustar
├── requirements.txt            # Dependencias de Python
├── RUBRICA_CHECKLIST.md        # Matriz de verificación (117 puntos)
├── VALIDACION_DATOS.md         # Auditoría de archivos de entrada
├── presentacion/
│   └── guion_video.md          # Guion para la presentación grupal
├── .gitignore
└── README.md
```

Los nombres y cantidades anteriores describen los archivos previstos en el proyecto. El notebook todavía utiliza `grupox` como nombre de referencia hasta que el equipo confirme el nombre definitivo.

## Requisitos y preparación del entorno

- **Python 3.10 o superior**, con una versión compatible con la versión de PySpark incluida en `requirements.txt`.
- **Java JDK 17 o 21**, según la compatibilidad de la versión de Spark instalada.
- **Visual Studio Code**, con las extensiones **Python** y **Jupyter**.
- Dependencias Python definidas en `requirements.txt`.

En una terminal **PowerShell**, ubicada en la raíz del repositorio:

```powershell
# 1. Crear un entorno virtual
py -m venv .venv

# 2. Actualizar pip e instalar dependencias
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

# 3. Confirmar disponibilidad de Java
java -version
```

Luego abre el notebook en VS Code, selecciona el intérprete/kernel **`.venv`** y ejecuta **Restart Kernel → Run All**. Los datos se cargan mediante rutas relativas a `data/`.

Si `java -version` no funciona, verifica la instalación del JDK y configura `JAVA_HOME` y `PATH`; después reinicia VS Code. Si aparecen problemas de compatibilidad entre Python, Java y PySpark, revisa las versiones efectivamente instaladas antes de modificar el notebook.

## Datos de entrada y consideraciones de calidad

Los archivos contienen particularidades que deben preservarse o tratarse explícitamente:

| Archivo | Particularidad identificada | Tratamiento documentado |
|---|---|---|
| `deportista.csv` | El registro con **ID 16587** presenta una coma final adicional | Lectura controlada sin alterar el archivo original |
| `deportista.csv` y `deportista2.csv` | Existen pesos decimales | Uso del tipo `double` para el peso |
| `evento.csv` | **280 registros** presentan envolturas de comillas | Normalización de una copia temporal |
| `resultados.csv` | Separador `;` y **9 valores `evento_id = #N/A`** | Conversión de identificadores inválidos a `NULL` |
| Datos de participaciones | **200 registros** tienen `equipo_id = 0`, sin coincidencia en equipos | Conservación de la fila con equipo no identificado |
| `juegos.json` | `temporada` contiene el año y `ciudad` identifica Verano/Invierno | Normalización semántica de nombres en el notebook |

Para integrar los resultados se priorizan **`left joins` desde la tabla de resultados**, con el fin de conservar las participaciones aunque falte una correspondencia válida en eventos o equipos. Los detalles de la auditoría se documentan en [`VALIDACION_DATOS.md`](VALIDACION_DATOS.md).

## Desarrollo del análisis en Spark

| Etapa | Procedimiento solicitado o implementado |
|---|---|
| **1. Configuración** | Inicialización de `SparkSession` y acceso a `SparkContext` |
| **2. RDD básicos** | Creación de RDD de deportistas con **6 particiones**, segundo RDD, unión, conteos y DataFrame de **7 columnas** |
| **3. Transformaciones RDD** | Filtrado de mayores de edad y mujeres; transformación de textos a mayúsculas |
| **4. Integración de datos** | Lectura de Evento, Resultado, Equipo y Juego; uniones y uso de `broadcast` cuando corresponde |
| **5. Particionamiento** | Aplicación de `repartition(5)` y verificación de las **5 particiones** |
| **6. Exploración del esquema** | Uso de `count()`, `dtypes` y `printSchema()` |
| **7. Variables derivadas** | Cálculo de **IMC** convirtiendo centímetros a metros; creación de `Descripción_sexo` |
| **8. Análisis y SQL** | Medallas por equipo y estadísticas de edad/altura por medalla, temporada y sexo; consultas **Spark SQL** |
| **9. Comunicación** | Video explicativo grupal de **7 a 10 minutos**; **pendiente de grabación** |

La implementación está contenida en el notebook principal. La conformidad final de cada apartado debe verificarse contrastando sus **salidas ejecutadas** con [`RUBRICA_CHECKLIST.md`](RUBRICA_CHECKLIST.md).

## Validación antes de la entrega

Antes de preparar el ZIP oficial, el equipo debe comprobar:

- [ ] Java y PySpark se inician correctamente y se crea la sesión Spark.
- [ ] El notebook se ejecuta **desde un kernel limpio**, sin excepciones.
- [ ] Los conteos, particiones y esquemas coinciden con los requisitos del taller.
- [ ] Las uniones conservan y explican los registros sin correspondencia.
- [ ] Las tablas, transformaciones, consultas SQL y gráficos/resultados quedan **visibles y guardados**.
- [ ] Se contrastan todos los criterios con la matriz de **117 puntos posibles**.
- [ ] Se graba el video grupal, con participación efectiva de todos los integrantes.
- [ ] El ZIP final contiene los archivos solicitados y se entrega en **Canvas**.

## Entrega oficial en Canvas

Según la guía proporcionada, la entrega requiere **un archivo ZIP** que contenga, como mínimo:

1. **Notebook ejecutable** (`.ipynb`) con resultados guardados.
2. **Video explicativo grupal** (`.mp4`) de **7 a 10 minutos**.

> **Importante:** el proyecto de desarrollo puede incluir datos, dependencias, auditoría y guion, pero **el guion no reemplaza la grabación**. Tampoco basta con enviar un enlace de GitHub si Canvas exige el ZIP.

La guía utiliza nombres de ejemplo como `mcdi502_s2_grupox.ipynb` y `mcdi502_s2_grupox_video.mp4`, pero también menciona un límite de **20 caracteres**, lo que genera una inconsistencia con esos ejemplos. El equipo debe confirmar con el docente cuál instrucción de nomenclatura tiene prioridad. Si se admiten nombres abreviados, `s2_g7.ipynb` y `s2_g7_video.mp4` cumplen ese límite.

## Integridad académica y alcance

Este repositorio reúne una implementación académica revisable. Corresponde al equipo **comprender y explicar las transformaciones**, verificar los resultados efectivamente ejecutados, acreditar la participación de sus integrantes y respetar las normas de autoría y citación de la asignatura.

La presencia de código, documentación o una lista de verificación **no equivale a una certificación de ejecución ni a una calificación obtenida**.

---

**Grupo 7 · MCDI502 · Big Data · Universidad Andrés Bello**

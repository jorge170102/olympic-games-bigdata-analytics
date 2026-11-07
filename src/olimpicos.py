"""Funciones reutilizables para el Taller I de Apache Spark (MCDI502)."""
import csv
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

def campos_deportista(linea):
    """Lee 7 campos; admite una coma sobrante final, sin alterar la fila original."""
    campos = next(csv.reader([linea]))
    if len(campos) == 8 and campos[-1].strip() == "":
        campos.pop()
    if len(campos) != 7 or not campos[0].strip().lstrip("\ufeff").isdigit():
        raise ValueError(f"Registro de deportista inválido: {linea!r}")
    return campos


def entero_o_none(valor):
    valor = valor.strip()
    return int(valor) if valor not in ("", "NA", "#N/A", "null", "None") else None


def decimal_o_none(valor):
    valor = valor.strip()
    return float(valor) if valor not in ("", "NA", "#N/A", "null", "None") else None


def parsear_deportista(linea):
    """Campos: id, nombre, genero, edad, altura (cm), peso (kg), equipo_id."""
    campos = campos_deportista(linea)
    return (
        int(campos[0].lstrip("\ufeff")), campos[1].strip(),
        entero_o_none(campos[2]), entero_o_none(campos[3]),
        entero_o_none(campos[4]), decimal_o_none(campos[5]),
        entero_o_none(campos[6])
    )


def preparar_eventos(entrada, salida):
    """Corrige solo las filas completas encapsuladas en comillas, conservando los registros."""
    filas = []
    reparadas = 0
    with open(entrada, "r", encoding="utf-8-sig", newline="") as f:
        lector = csv.reader(f)
        cabecera = next(lector)
        if cabecera != ["cod_evento", "evento", "deporte_id"]:
            raise ValueError(f"Cabecera inesperada en eventos: {cabecera}")
        for indice, fila in enumerate(lector, start=2):
            if len(fila) == 1:
                fila = next(csv.reader([fila[0]]))
                reparadas += 1
            if len(fila) != 3:
                raise ValueError(f"Fila de eventos {indice} inválida: {fila}")
            filas.append(fila)
    with open(salida, "w", encoding="utf-8", newline="") as f:
        escritor = csv.writer(f)
        escritor.writerow(cabecera)
        escritor.writerows(filas)
    return len(filas), reparadas


def calcular_columnas(df):
    return (df.withColumn("IMC", F.when((F.col("altura") > 0) & (F.col("peso") > 0),
                  F.round(F.col("peso") / F.pow(F.col("altura") / F.lit(100), F.lit(2)), 2))
                  .otherwise(F.lit(None).cast("double")))
            .withColumn("Descripción_sexo", F.when(F.col("sexo") == 1, "hombre")
                  .when(F.col("sexo") == 2, "mujer").otherwise("no informado")))


def medallas_equipo(df):
    return (df.filter(F.col("medalla").isin("Gold", "Silver", "Bronze"))
            .groupBy("equipo_id", "nombre_equipo", "sigla")
            .pivot("medalla", ["Gold", "Silver", "Bronze"]).count()
            .fillna(0, subset=["Gold", "Silver", "Bronze"])
            .withColumnRenamed("Gold", "oro").withColumnRenamed("Silver", "plata")
            .withColumnRenamed("Bronze", "bronce")
            .withColumn("total", F.col("oro") + F.col("plata") + F.col("bronce"))
            .orderBy(F.desc("total"), "nombre_equipo"))


def estadisticas(df, variable, agrupacion, nombre=None):
    """Suma, media, máximo y mínimo excluyendo ceros/no informados."""
    prefijo = nombre or variable
    return (df.filter(F.col(variable) > 0).groupBy(agrupacion)
            .agg(F.sum(variable).alias(f"suma_{prefijo}"),
                 F.count(variable).alias("cantidad_deportistas"),
                 F.round(F.avg(variable), 2).alias(f"promedio_{prefijo}"),
                 F.max(variable).alias(f"maximo_{prefijo}"),
                 F.min(variable).alias(f"minimo_{prefijo}"))
            .orderBy(agrupacion))

"""
Glue job (Spark): convierte cada CSV de la zona raw a Parquet en la zona curated.

Por cada carpeta s3://<bucket>/raw/<tabla>/ :
  1. Lee el CSV con encabezado e inferencia de tipos.
  2. Convierte el marcador de nulos del dataset ("\\N") en NULL real.
  3. Renombra columnas a snake_case (raceId -> race_id) para Athena.
  4. Limpia nulos:
       - elimina columnas 100 % vacías,
       - elimina filas totalmente vacías y filas duplicadas,
       - elimina filas sin llave (columnas *_id nulas).
     Con --NULL_MODE any elimina cualquier fila que tenga al menos un nulo
     (útil para ver en clase cuántos datos se pierden con un dropna() ciego).
  5. Escribe Parquet comprimido con Snappy en s3://<bucket>/curated/<tabla>/.

Parámetros del job:
  --SOURCE_PATH  s3://<bucket>/raw/
  --TARGET_PATH  s3://<bucket>/curated/
  --NULL_MODE    keys (por defecto) | any
"""
import re
import sys

from pyspark.sql import SparkSession, functions as F

try:  # Dentro de AWS Glue
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_PATH", "TARGET_PATH", "NULL_MODE"])
    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
except ImportError:  # Ejecución local para pruebas: python csv_to_parquet.py <src> <dst> [keys|any]
    args = {
        "SOURCE_PATH": sys.argv[1],
        "TARGET_PATH": sys.argv[2],
        "NULL_MODE": sys.argv[3] if len(sys.argv) > 3 else "keys",
    }
    spark = SparkSession.builder.appName("csv_to_parquet_local").getOrCreate()
    job = None

NULL_TOKEN = "\\N"
source = args["SOURCE_PATH"].rstrip("/") + "/"
target = args["TARGET_PATH"].rstrip("/") + "/"
null_mode = args["NULL_MODE"].lower()


def snake_case(name: str) -> str:
    name = re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", name.strip())
    return re.sub(r"[^0-9a-zA-Z_]", "_", name).lower()


def list_tables(path: str) -> list:
    """Devuelve las subcarpetas de primer nivel (una por tabla) bajo `path`."""
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = fs_path.getFileSystem(conf)
    return sorted(s.getPath().getName() for s in fs.listStatus(fs_path) if s.isDirectory())


tables = list_tables(source)
print(f"Tablas encontradas en {source}: {tables}")

for table in tables:
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("nullValue", NULL_TOKEN)
        # Fechas tipo 2009-03-29 -> DATE. Horas sueltas ("06:00:00") quedan como texto:
        # sin este formato estricto Spark las convertiría en timestamps del día de hoy.
        .option("dateFormat", "yyyy-MM-dd")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .csv(f"{source}{table}/")
    )
    df = df.toDF(*[snake_case(c) for c in df.columns])

    # Texto: quitar espacios y tratar "" o "\N" que hayan quedado como NULL.
    for col, dtype in df.dtypes:
        if dtype == "string":
            trimmed = F.trim(F.col(col))
            df = df.withColumn(col, F.when(trimmed.isin("", NULL_TOKEN), None).otherwise(trimmed))

    rows_in = df.count()

    # Columnas 100 % vacías: no aportan nada.
    non_null = df.select([F.count(F.col(c)).alias(c) for c in df.columns]).first().asDict()
    empty_cols = [c for c, n in non_null.items() if n == 0]
    df = df.drop(*empty_cols)

    df = df.dropna(how="all").dropDuplicates()
    if null_mode == "any":
        df = df.dropna(how="any")
    else:
        key_cols = [c for c in df.columns if c.endswith("_id")]
        if key_cols:
            df = df.dropna(how="any", subset=key_cols)

    df = df.cache()
    rows_out = df.count()
    df.coalesce(1 if rows_out < 1_000_000 else 8).write.mode("overwrite").option(
        "compression", "snappy"
    ).parquet(f"{target}{table}/")
    print(
        f"{table}: {rows_in} filas -> {rows_out} filas "
        f"({rows_in - rows_out} eliminadas), columnas vacías eliminadas: {empty_cols or 'ninguna'}"
    )
    df.unpersist()

if job:
    job.commit()

# Laboratorio: un data lake de Fórmula 1 en AWS

Construyes un data lake serverless con 75 años de datos de Fórmula 1: lo guardas en **Amazon S3**, lo catalogas y limpias con **AWS Glue** (Spark), y lo analizas con SQL en **Amazon Athena**. Al final, si quieres, lo visualizas en **Amazon Quick Sight**.

```
 Kaggle (CSV) ──► S3 raw/ ──► Glue crawler ──► Glue job (Spark) ──► S3 curated/ (Parquet) ──► Glue crawler ──► Athena ──► Quick Sight (opcional)
```

| Parte | Qué haces | Tiempo |
|---|---|---|
| 0 | Descargar el dataset | 5 min |
| A | Ruta manual en la consola: bucket, carga, tablas y consultas | 20 min |
| B | Ruta con código: desplegar todo con AWS CDK (Python) | 15 min |
| C | Ingeniería de datos: el job de Spark que convierte a Parquet y limpia nulos | 15 min |
| D | *(Opcional, con costo)* Dashboard en Amazon Quick Sight | 15 min |
| E | Limpiar todo | 5 min |

> **Costo estimado de las partes A a C:** menos de USD 1. Athena cobra USD 5 por TB escaneado (este dataset pesa ~20 MB), el job de Glue usa 2 workers unos pocos minutos (USD 0,44 por DPU-hora) y cada ejecución de crawler factura un mínimo de 10 minutos. El workgroup de Athena corta cualquier consulta que escanee más de 1 GB. **La parte D sí puede costar dinero**: lee su nota antes de empezar.

---

## Parte 0 · Descargar el dataset

1. Entra a [Formula 1 World Championship (1950 - 2024)](https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020) en Kaggle (necesitas una cuenta gratuita) y pulsa **Download**. Obtienes un `archive.zip` con ~14 CSV.
2. Descomprímelo y copia los CSV **planos** dentro de la carpeta `data/` de este proyecto:

```bash
# Linux, macOS o AWS CloudShell (desde la carpeta de este proyecto)
unzip archive.zip -d data
ls data        # circuits.csv  constructors.csv  drivers.csv  races.csv  results.csv ...
```

```powershell
# Windows PowerShell
Expand-Archive archive.zip -DestinationPath data
```

No hace falta reorganizarlos: en la parte B el stack de CDK los reparte en
`.build/raw/<tabla>/<tabla>.csv` y los sube a `s3://.../raw/` con un
`BucketDeployment`. Athena y Glue tratan cada carpeta de S3 como una tabla, y de
eso se encarga el stack.

Para la parte A (ruta manual) sí necesitas las carpetas. Sintetiza una vez
(`cdk synth`) y usa la carpeta `.build/raw/` que queda generada, o créalas a
mano.

**Dato curioso del dataset:** los valores vacíos vienen escritos como `\N` (herencia de una exportación de MySQL). Lo vas a ver en la parte A y lo vas a limpiar en la parte C.

---

## Parte A · Ruta manual en la consola

### A1. Crear el bucket
1. Abre **S3 → Create bucket**.
2. Nombre: `f1-manual-<tus-iniciales>-<numero>` (debe ser único en todo AWS). Región: la misma que usarás en Athena (p. ej. `us-east-1`).
3. Deja **Block all public access** activado y pulsa **Create bucket**.

### A2. Subir los archivos
1. Entra al bucket → **Create folder** → `raw`.
2. Dentro de `raw/`, pulsa **Upload → Add folder** y sube las carpetas `races`, `drivers` y `results` de `.build/raw/` (puedes subir todas). Esa carpeta la genera `cdk synth`; si aún no la tienes, ejecútalo una vez o sube los CSV sueltos creando cada carpeta a mano.
3. Verifica que quede así: `raw/races/races.csv`, `raw/drivers/drivers.csv`, `raw/results/results.csv`.

### A3. Preparar Athena
1. Abre **Athena → Query editor**. Si es la primera vez, ve a **Settings → Manage** y en *Query result location* pon `s3://<tu-bucket>/athena-results/`.
2. Crea una base de datos:

```sql
CREATE DATABASE f1_manual;
```

### A4. Crear las tablas sobre los CSV
Los CSV traen comillas y los nulos como `\N`, así que usamos `OpenCSVSerde` y declaramos todo como `STRING`. Reemplaza `<tu-bucket>` y ejecuta **una sentencia a la vez** (las comillas invertidas protegen nombres reservados como `date`):

```sql
CREATE EXTERNAL TABLE f1_manual.races (
  raceId STRING, `year` STRING, `round` STRING, circuitId STRING,
  name STRING, `date` STRING, `time` STRING, url STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
LOCATION 's3://<tu-bucket>/raw/races/'
TBLPROPERTIES ('skip.header.line.count' = '1');
```

```sql
CREATE EXTERNAL TABLE f1_manual.drivers (
  driverId STRING, driverRef STRING, `number` STRING, code STRING,
  forename STRING, surname STRING, dob STRING, nationality STRING, url STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
LOCATION 's3://<tu-bucket>/raw/drivers/'
TBLPROPERTIES ('skip.header.line.count' = '1');
```

```sql
CREATE EXTERNAL TABLE f1_manual.results (
  resultId STRING, raceId STRING, driverId STRING, constructorId STRING,
  `number` STRING, grid STRING, `position` STRING, positionText STRING,
  positionOrder STRING, points STRING, laps STRING, `time` STRING,
  milliseconds STRING, fastestLap STRING, `rank` STRING, fastestLapTime STRING,
  fastestLapSpeed STRING, statusId STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
LOCATION 's3://<tu-bucket>/raw/results/'
TBLPROPERTIES ('skip.header.line.count' = '1');
```

> Las versiones nuevas del dataset traen columnas extra al final de `races` (horarios de prácticas y sprint). No pasa nada: Athena ignora las columnas que no declares.

### A5. Consultar
```sql
-- ¿Quién ha ganado más carreras?
SELECT d.forename || ' ' || d.surname AS piloto, COUNT(*) AS victorias
FROM f1_manual.results r
JOIN f1_manual.drivers d ON r.driverId = d.driverId
WHERE r.position = '1'
GROUP BY 1
ORDER BY victorias DESC
LIMIT 10;
```

```sql
-- Mira los nulos "\N" y por qué todo es texto
SELECT position, time, fastestLapTime FROM f1_manual.results LIMIT 20;

-- Para sumar hay que convertir a mano en cada consulta
SELECT SUM(TRY_CAST(NULLIF(points, '\N') AS DOUBLE)) AS puntos_totales
FROM f1_manual.results;
```

**Para pensar:** tener que limpiar y convertir tipos en *cada* consulta es lento, propenso a errores y escanea el CSV completo cada vez. Eso es lo que resuelve la zona *curated* de la parte C.

---

## Parte B · Ruta con código: AWS CDK en Python

La opción más fácil es **AWS CloudShell** (icono `>_` en la consola): ya trae Python, Node.js, AWS CLI y CDK.

### B1. Subir el proyecto
1. Comprime la carpeta `lab-f1-cdk` (incluida tu carpeta `data/` con los CSV) como `lab-f1-cdk.zip`.
2. En CloudShell: **Actions → Upload file** → `lab-f1-cdk.zip`, y luego:

```bash
unzip lab-f1-cdk.zip && cd lab-f1-cdk
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

```powershell
# Windows PowerShell, en tu computador
py -3.12 -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

> `cdk.json` invoca el intérprete como `python`, que es el que crea el entorno
> virtual en los tres sistemas. **Activa el `.venv` antes de cualquier comando
> `cdk`**: sin activarlo, CDK usa el Python del sistema, no encuentra
> `aws-cdk-lib` y `cdk deploy` falla antes de empezar.

> ¿Trabajas en tu computador? Necesitas Python 3.10+ (probado con 3.12),
> Node.js 20+, AWS CLI configurado y `npm install -g aws-cdk`.

### B2. Desplegar
```bash
# Linux, macOS o CloudShell (con el .venv activado)
source .venv/bin/activate
aws sts get-caller-identity   # comprueba que tienes credenciales vigentes
cdk bootstrap                 # una sola vez por cuenta y región
cdk synth                     # opcional: ver la plantilla de CloudFormation generada
cdk deploy                    # confirma con "y"
```

```powershell
# Windows PowerShell
.venv\Scripts\Activate.ps1
$env:AWS_PROFILE = "<tu-perfil>"   # si no usas el perfil default
aws sts get-caller-identity
cdk bootstrap
cdk synth
cdk deploy
```

> Si `Activate.ps1` da error de *execution policy*, actívalo sólo para esta
> sesión con `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass`, o
> evita activarlo poniendo el venv al frente del PATH:
> `$env:PATH = "$PWD\.venv\Scripts;$env:PATH"`.

Si usas IAM Identity Center (SSO) y la sesión caducó, renuévala primero con
`aws sso login --profile <tu-perfil>`.

El stack toma la cuenta y la región del perfil activo (`CDK_DEFAULT_ACCOUNT` y
`CDK_DEFAULT_REGION`), así que el nombre del bucket queda fijo desde el synth.

Qué crea el stack (`f1_datalake/datalake_stack.py`):

| Recurso | Nombre | Para qué |
|---|---|---|
| Bucket S3 | `f1-datalake-<cuenta>-<region>` | Zonas `raw/`, `curated/`, `athena-results/` y `tmp/` |
| Carga de datos (`BucketDeployment`) | `RawDataDeployment` | Sube los CSV de `data/` a `raw/<tabla>/` al desplegar |
| Bases de datos Glue | `f1_raw`, `f1_curated` | Catálogo de las dos zonas |
| Crawlers Glue | `f1-raw-crawler`, `f1-curated-crawler` | Descubren esquemas y crean una tabla por carpeta |
| Job Glue (Spark 3.5, Glue 5.0) | `f1-csv-to-parquet` | CSV → Parquet + limpieza de nulos |
| Workflow Glue | `f1-datalake-pipeline` | Encadena crawler raw → job → crawler curated |
| Workgroup Athena | `f1-datalake` | Resultados en S3, corte a 1 GB por consulta y 7 consultas guardadas |

### B3. Correr el pipeline
Los CSV ya están en `raw/`: el `BucketDeployment` del stack los subió durante
`cdk deploy`. La salida `RawTables` lista las tablas que se cargaron. Sólo falta
arrancar el pipeline (el comando exacto está en la salida `RunPipeline`):

```bash
aws glue start-workflow-run --name f1-datalake-pipeline
```

> Si añades o cambias CSV en `data/`, vuelve a ejecutar `cdk deploy` y el
> `BucketDeployment` los sube de nuevo. Está configurado con `prune=False`, así
> que no borra nada que no venga de `data/`.
>
> ¿Necesitas subirlos a mano (por ejemplo, si la Lambda de carga falla)? Usa el
> comando de la salida `UploadCommand`:
> `aws s3 sync ./.build/raw s3://f1-datalake-<cuenta>-<region>/raw/`

Sigue el avance en **Glue → Workflows → f1-datalake-pipeline → History → View run details**. Tarda unos 5 a 8 minutos: dos crawlers y un job de Spark.

### B4. Consultar en Athena
1. En **Athena → Query editor**, elige el workgroup **f1-datalake** (arriba a la derecha).
2. Pestaña **Saved queries**: ahí están las 7 consultas de `athena/queries.sql`. Ábrelas y ejecútalas:
   - 01 Pilotos con más victorias
   - 02 Campeón de constructores por temporada
   - 03 Pilotos colombianos en la F1 (spoiler: Juan Pablo Montoya)
   - 04 ¿Cuánto vale la pole? Victorias desde P1 por década
   - 05 Tiempo en pit lane por equipo (última temporada)
   - 06 Circuitos con más Grandes Premios
   - 07 CSV vs Parquet: compara "Data scanned"
3. En la 07 ejecuta las dos consultas por separado y compara **Data scanned**: Parquet lee solo las columnas que necesita y viene comprimido.

---

## Parte C · Ingeniería de datos: el job de Spark

Abre `glue_jobs/csv_to_parquet.py`, o en la consola **Glue → ETL jobs → f1-csv-to-parquet → Script**. Para cada carpeta de `raw/` el job:

1. Lee el CSV con encabezado e inferencia de tipos (`int`, `double`, `date`).
2. Convierte `\N` y textos vacíos en `NULL` reales.
3. Renombra columnas a `snake_case` (`raceId` → `race_id`), que es lo que Athena espera.
4. Limpia nulos: elimina columnas 100 % vacías, filas totalmente vacías, filas duplicadas y filas sin llave (`*_id` nulo).
5. Escribe Parquet comprimido con Snappy en `curated/<tabla>/`.

Los conteos por tabla quedan en **CloudWatch Logs** (en el job: **Runs → Output logs**), con líneas como `results: 25220 filas -> 25220 filas`.

### Experimento: ¿y si borro *todas* las filas con algún nulo?
El job acepta `--NULL_MODE any`, el típico `dropna()` sin pensar. Ejecútalo hacia otra carpeta:

```bash
BUCKET=f1-datalake-<cuenta>-<region>
aws glue start-job-run --job-name f1-csv-to-parquet --arguments \
  "{\"--NULL_MODE\":\"any\",\"--TARGET_PATH\":\"s3://$BUCKET/experimento-dropna/\"}"
```

Revisa los logs. En la versión del dataset que probamos, `results` pasa de 25.220 filas a 3.317: **se pierde el 87 % de los resultados**, porque un piloto que abandona no tiene tiempo final ni vuelta rápida. Y `drivers` pierde 803 de 853 pilotos, porque los de antes de 2014 no tienen número permanente. Moraleja: "quitar nulos" es una decisión de negocio, no un comando.

**Reto:** agrega un paso que particione `lap_times` por `race_id` (`.partitionBy("race_id")`), vuelve a correr el workflow y compara cuánto escanea `SELECT * FROM f1_curated.lap_times WHERE race_id = 1100`.

---

## Parte D · *(Opcional)* Dashboard en Amazon Quick Sight

> **Nota de costo:** Quick Sight (ahora parte de **Amazon Quick**) se cobra por usuario y por mes. Hoy un *Author* cuesta USD 24/mes y un *Reader* USD 3/mes; los usuarios *Pro* y las funciones de preguntas en lenguaje natural agregan una tarifa de USD 250/mes por cuenta. Revisa en [aws.amazon.com/quick/quicksight/pricing](https://aws.amazon.com/quick/quicksight/pricing/) si tu cuenta tiene prueba gratuita **antes** de suscribirte, usa solo un usuario Author sin Pro y cancela la suscripción al terminar (parte E). Si no quieres arriesgar costos, tu profesor lo hará como demo.

1. Abre **Amazon Quick** y suscríbete con un usuario Author (sin Pro). En la configuración de seguridad y permisos, autoriza **Amazon Athena** y el bucket `f1-datalake-...`.
2. **Datasets → New dataset → Athena**, workgroup **f1-datalake**, base de datos `f1_curated`.
3. Elige **Use custom SQL** y pega la consulta 01 o la 04 de `athena/queries.sql`. Impórtala a SPICE (10 GB incluidos por Author).
4. Crea un análisis:
   - Barras horizontales: `piloto` vs `victorias` (consulta 01).
   - Línea: `decada` vs `pct` (consulta 04), ¿la pole importa más hoy que en los 50?
5. **Publish → Dashboard**.

---

## Parte E · Limpiar todo

```bash
cdk destroy          # borra bucket (con sus objetos), Glue, Athena y roles
```

- Borra el bucket de la parte A: S3 → selecciónalo → **Empty** → **Delete**. En Athena: `DROP DATABASE f1_manual CASCADE;`
- Si hiciste la parte D: en Amazon Quick, **Manage account → Account settings → Delete account** (o elimina tu usuario) para que no siga cobrando.

---

## Problemas frecuentes

| Síntoma | Causa y solución |
|---|---|
| `cdk deploy` dice que falta bootstrap | Ejecuta `cdk bootstrap` en esa cuenta y región. |
| `ModuleNotFoundError: No module named 'aws_cdk'` | No activaste el `.venv`. Actívalo (`.venv\Scripts\Activate.ps1` o `source .venv/bin/activate`) y repite `pip install -r requirements.txt`. |
| `ExpiredToken` / `Unable to locate credentials` | La sesión de AWS caducó. Renuévala con `aws sso login --profile <tu-perfil>` y comprueba con `aws sts get-caller-identity`. |
| `raw/` quedó vacío después de `cdk deploy` | La carpeta `data/` no tenía CSV al sintetizar. La salida `RawTables` lo avisa. Copia los CSV a `data/` y repite `cdk deploy`. |
| El crawler termina pero no aparecen tablas | Los CSV no están en `raw/<tabla>/`. Revisa con `aws s3 ls s3://<bucket>/raw/ --recursive`. |
| `Insufficient Lake Formation permission(s)` | Tu cuenta tiene Lake Formation activado para el catálogo. En **Lake Formation → Data permissions** dale a tu usuario y al rol `F1DataLakeStack-GlueRole...` permisos sobre `f1_raw` y `f1_curated`. |
| Athena: `Query exhausted resources` o corte por 1 GB | Estás consultando `f1_raw` sin filtro o hiciste un `CROSS JOIN` accidental. |
| Athena no muestra las Saved queries | Selecciona el workgroup **f1-datalake**. |
| El job falla con `AccessDenied` | Verifica que los archivos estén en el bucket del stack, no en el de la parte A. |

## Estructura del proyecto

```
lab-f1-cdk/
├── app.py                          # punto de entrada de CDK
├── cdk.json
├── requirements.txt
├── data/                           # TÚ pones aquí los CSV de Kaggle (planos)
├── .build/raw/                     # generado: un CSV por carpeta, listo para S3
├── f1_datalake/datalake_stack.py   # toda la infraestructura
├── glue_jobs/csv_to_parquet.py     # job de Spark (también corre local con pyspark)
└── athena/queries.sql              # consultas que se publican como Saved queries
```

El job también corre en tu máquina con `pip install pyspark`:

```bash
# Linux, macOS o Git Bash
python glue_jobs/csv_to_parquet.py "file://$PWD/.build/raw/" "file://$PWD/.build/curated/"
```

```powershell
# Windows PowerShell
python glue_jobs/csv_to_parquet.py "file:///$($PWD.Path -replace '\\','/')/.build/raw/" `
                                   "file:///$($PWD.Path -replace '\\','/')/.build/curated/"
```

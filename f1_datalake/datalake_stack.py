"""Data lake serverless de F1: S3 + Glue (catálogo, crawlers, job Spark) + Athena."""
import json
import re
import shutil
from pathlib import Path
from typing import Optional

from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Size,
    Stack,
    aws_athena as athena,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_assets as s3_assets,
    aws_s3_deployment as s3_deployment,
)
from constructs import Construct

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
STAGE_DIR = ROOT / ".build" / "raw"


def _load_named_queries() -> list:
    """Lee athena/queries.sql y separa cada bloque '-- name: ...'."""
    text = (ROOT / "athena" / "queries.sql").read_text(encoding="utf-8")
    queries = []
    for block in text.split("-- name: ")[1:]:
        name, body = block.split("\n", 1)
        queries.append((name.strip(), body.strip()))
    return queries


def _table_name(stem: str) -> str:
    """Nombre de tabla válido para Glue: minúsculas, sin espacios ni símbolos."""
    stem = re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", stem.strip())
    return re.sub(r"[^0-9a-zA-Z_]", "_", stem).lower()


def _stage_raw_data() -> Optional[Path]:
    """Reorganiza los CSV de data/ en una carpeta por tabla, lista para subir a raw/.

    Acepta tanto data/results.csv como data/raw/results/results.csv; en ambos casos
    deja .build/raw/results/results.csv. Los crawlers usan TableLevelConfiguration 3
    (bucket/raw/<tabla>), asi que cada CSV necesita su propia carpeta para
    convertirse en una tabla independiente del catalogo.
    """
    if not DATA_DIR.is_dir():
        return None
    csv_files = sorted(p for p in DATA_DIR.rglob("*.csv") if p.is_file())
    if not csv_files:
        return None

    shutil.rmtree(STAGE_DIR, ignore_errors=True)
    for csv_file in csv_files:
        table = _table_name(csv_file.stem)
        destination = STAGE_DIR / table
        destination.mkdir(parents=True, exist_ok=True)
        shutil.copy2(csv_file, destination / (table + ".csv"))
    return STAGE_DIR


class F1DataLakeStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ---------- Almacenamiento: un bucket con zonas por prefijo ----------
        # raw/        CSV tal como vienen de Kaggle (una carpeta por tabla)
        # curated/    Parquet limpio, generado por el job de Glue
        # athena-results/  resultados de las consultas
        # tmp/        espacio temporal del job de Spark (--TempDir)
        bucket = s3.Bucket(
            self,
            "DataLakeBucket",
            bucket_name=f"f1-datalake-{self.account}-{self.region}",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,  # laboratorio: se borra con `cdk destroy`
            auto_delete_objects=True,
            lifecycle_rules=[
                s3.LifecycleRule(prefix="athena-results/", expiration=Duration.days(7)),
                s3.LifecycleRule(prefix="tmp/", expiration=Duration.days(7)),
            ],
        )
        raw_path = f"s3://{bucket.bucket_name}/raw/"
        curated_path = f"s3://{bucket.bucket_name}/curated/"
        temp_path = f"s3://{bucket.bucket_name}/tmp/"

        # ---------- Carga de los CSV: data/ -> s3://<bucket>/raw/<tabla>/ ----------
        # Si data/ todavia no tiene CSV el stack sigue sintetizando y los datos se
        # pueden subir a mano con `aws s3 sync` (ver la salida UploadCommand).
        staged = _stage_raw_data()
        raw_data_deployment = None
        tables: list = []
        if staged is not None:
            tables = sorted(p.name for p in staged.iterdir() if p.is_dir())
            raw_data_deployment = s3_deployment.BucketDeployment(
                self,
                "RawDataDeployment",
                sources=[s3_deployment.Source.asset(str(staged))],
                destination_bucket=bucket,
                destination_key_prefix="raw/",
                prune=False,  # no borra nada que no venga de data/
                retain_on_delete=False,  # `cdk destroy` limpia tambien los CSV
                memory_limit=1024,  # lap_times.csv ronda los 20 MB
                ephemeral_storage_size=Size.gibibytes(2),
            )

        # ---------- Catálogo de datos de Glue ----------
        databases = {}
        for zone, description in [
            ("raw", "F1 - CSV originales (zona raw)"),
            ("curated", "F1 - Parquet limpio (zona curated)"),
        ]:
            databases[zone] = glue.CfnDatabase(
                self,
                f"Db{zone.title()}",
                catalog_id=self.account,
                database_input=glue.CfnDatabase.DatabaseInputProperty(
                    name=f"f1_{zone}", description=description
                ),
            )

        # ---------- Rol que usan los crawlers y el job ----------
        glue_role = iam.Role(
            self,
            "GlueRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ],
        )
        bucket.grant_read_write(glue_role)

        # ---------- Crawlers: una tabla por carpeta (nivel 3 = bucket/zona/tabla) ----------
        crawler_config = json.dumps(
            {
                "Version": 1.0,
                "Grouping": {"TableLevelConfiguration": 3},
                "CrawlerOutput": {"Tables": {"AddOrUpdateBehavior": "MergeNewColumns"}},
            }
        )
        crawlers = {}
        for zone, path in [("raw", raw_path), ("curated", curated_path)]:
            crawlers[zone] = glue.CfnCrawler(
                self,
                f"Crawler{zone.title()}",
                name=f"f1-{zone}-crawler",
                role=glue_role.role_arn,
                database_name=f"f1_{zone}",
                targets=glue.CfnCrawler.TargetsProperty(
                    s3_targets=[glue.CfnCrawler.S3TargetProperty(path=path)]
                ),
                configuration=crawler_config,
                schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                    update_behavior="UPDATE_IN_DATABASE", delete_behavior="DEPRECATE_IN_DATABASE"
                ),
            )
            crawlers[zone].add_resource_dependency(databases[zone])
            # El rol necesita su politica inline antes de que Glue valide el crawler.
            crawlers[zone].node.add_dependency(glue_role)

        # El crawler de raw solo tiene sentido cuando los CSV ya estan en el bucket.
        if raw_data_deployment is not None:
            crawlers["raw"].node.add_dependency(raw_data_deployment)

        # ---------- Job de Spark: CSV -> Parquet sin nulos ----------
        script = s3_assets.Asset(
            self, "JobScript", path=str(ROOT / "glue_jobs" / "csv_to_parquet.py")
        )
        script.grant_read(glue_role)

        job = glue.CfnJob(
            self,
            "CsvToParquetJob",
            name="f1-csv-to-parquet",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl", python_version="3", script_location=script.s3_object_url
            ),
            glue_version="5.0",
            worker_type="G.1X",
            number_of_workers=2,  # el mínimo: el dataset completo pesa ~20 MB
            timeout=20,  # minutos
            max_retries=0,
            execution_class="STANDARD",
            default_arguments={
                "--SOURCE_PATH": raw_path,
                "--TARGET_PATH": curated_path,
                "--NULL_MODE": "keys",
                "--TempDir": temp_path,
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--job-bookmark-option": "job-bookmark-disable",
            },
        )
        job.node.add_dependency(glue_role)

        # ---------- Workflow: crawler raw -> job -> crawler curated ----------
        workflow = glue.CfnWorkflow(
            self, "Pipeline", name="f1-datalake-pipeline", description="Raw -> Parquet -> catalogo"
        )
        start = glue.CfnTrigger(
            self,
            "StartTrigger",
            name="f1-start",
            type="ON_DEMAND",
            workflow_name=workflow.name,
            actions=[glue.CfnTrigger.ActionProperty(crawler_name=crawlers["raw"].name)],
        )
        after_raw = glue.CfnTrigger(
            self,
            "AfterRawCrawler",
            name="f1-after-raw-crawler",
            type="CONDITIONAL",
            start_on_creation=True,
            workflow_name=workflow.name,
            predicate=glue.CfnTrigger.PredicateProperty(
                conditions=[
                    glue.CfnTrigger.ConditionProperty(
                        logical_operator="EQUALS",
                        crawler_name=crawlers["raw"].name,
                        crawl_state="SUCCEEDED",
                    )
                ]
            ),
            actions=[glue.CfnTrigger.ActionProperty(job_name=job.name)],
        )
        after_job = glue.CfnTrigger(
            self,
            "AfterJob",
            name="f1-after-job",
            type="CONDITIONAL",
            start_on_creation=True,
            workflow_name=workflow.name,
            predicate=glue.CfnTrigger.PredicateProperty(
                conditions=[
                    glue.CfnTrigger.ConditionProperty(
                        logical_operator="EQUALS", job_name=job.name, state="SUCCEEDED"
                    )
                ]
            ),
            actions=[glue.CfnTrigger.ActionProperty(crawler_name=crawlers["curated"].name)],
        )
        for trigger in (start, after_raw, after_job):
            trigger.add_resource_dependency(workflow)
        start.add_resource_dependency(crawlers["raw"])
        after_raw.add_resource_dependency(crawlers["raw"])
        after_raw.add_resource_dependency(job)
        after_job.add_resource_dependency(job)
        after_job.add_resource_dependency(crawlers["curated"])

        # ---------- Athena: workgroup con límite de escaneo + consultas guardadas ----------
        workgroup = athena.CfnWorkGroup(
            self,
            "WorkGroup",
            name="f1-datalake",
            description="Workgroup del laboratorio F1",
            recursive_delete_option=True,
            state="ENABLED",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration=True,
                publish_cloud_watch_metrics_enabled=True,
                bytes_scanned_cutoff_per_query=1_000_000_000,  # 1 GB por consulta: freno de costos
                engine_version=athena.CfnWorkGroup.EngineVersionProperty(
                    selected_engine_version="AUTO"
                ),
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{bucket.bucket_name}/athena-results/",
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_S3"
                    ),
                ),
            ),
        )
        for i, (name, sql) in enumerate(_load_named_queries()):
            query = athena.CfnNamedQuery(
                self,
                f"Query{i:02d}",
                name=name,
                database="f1_curated",
                work_group=workgroup.name,
                query_string=sql,
            )
            query.add_resource_dependency(workgroup)
            query.add_resource_dependency(databases["curated"])

        # ---------- Salidas ----------
        CfnOutput(self, "BucketName", value=bucket.bucket_name)
        CfnOutput(
            self,
            "RawTables",
            value=", ".join(tables) if tables else "data/ vacia: sube los CSV y vuelve a desplegar",
            description="Tablas subidas a raw/ desde la carpeta data/",
        )
        CfnOutput(
            self,
            "UploadCommand",
            value=f"aws s3 sync ./.build/raw {raw_path}",
            description="Solo si hace falta volver a subir los CSV a mano",
        )
        CfnOutput(self, "RunPipeline", value=f"aws glue start-workflow-run --name {workflow.name}")
        CfnOutput(self, "AthenaWorkGroup", value=workgroup.name)

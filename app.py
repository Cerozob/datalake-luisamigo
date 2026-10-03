#!/usr/bin/env python3
import os

import aws_cdk as cdk

from f1_datalake.datalake_stack import F1DataLakeStack

app = cdk.App()

# Cuenta y región explícitas (las toma del perfil de AWS activo). Hacen falta para
# que el nombre del bucket sea un valor concreto y no un token sin resolver.
env = cdk.Environment(
    account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region=os.environ.get("CDK_DEFAULT_REGION"),
)

F1DataLakeStack(
    app,
    "F1DataLakeStack",
    env=env,
    description="Laboratorio: data lake serverless con datos de Formula 1",
)
app.synth()

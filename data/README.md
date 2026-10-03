# Carpeta `data/`

Pon aquí los CSV del dataset de Kaggle
[Formula 1 World Championship (1950 - 2024)](https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020).

Basta con dejarlos planos, tal como salen del `archive.zip`:

```
data/
├── circuits.csv
├── constructor_results.csv
├── constructor_standings.csv
├── constructors.csv
├── driver_standings.csv
├── drivers.csv
├── lap_times.csv
├── pit_stops.csv
├── qualifying.csv
├── races.csv
├── results.csv
├── seasons.csv
├── sprint_results.csv
└── status.csv
```

Al sintetizar, el stack reorganiza cada CSV en su propia carpeta
(`.build/raw/<tabla>/<tabla>.csv`) y un `BucketDeployment` los sube a
`s3://f1-datalake-<cuenta>-<region>/raw/`. Cada carpeta se vuelve una tabla
independiente en el catálogo de Glue.

También funciona si los dejas ya organizados en subcarpetas
(`data/raw/results/results.csv`): el stack busca `*.csv` de forma recursiva.

Después de añadir o cambiar CSV, vuelve a ejecutar `cdk deploy`.

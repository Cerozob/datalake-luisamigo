-- Consultas de ejemplo sobre la zona curated (Parquet) del data lake F1.
-- Cada bloque empieza con "-- name:"; el stack de CDK las crea como
-- "Saved queries" en el workgroup de Athena.

-- name: 01 Pilotos con más victorias
SELECT d.forename || ' ' || d.surname AS piloto,
       d.nationality                  AS nacionalidad,
       COUNT(*)                       AS victorias
FROM f1_curated.results r
JOIN f1_curated.drivers d ON r.driver_id = d.driver_id
WHERE r.position = 1
GROUP BY 1, 2
ORDER BY victorias DESC
LIMIT 10;

-- name: 02 Campeón de constructores por temporada
WITH ultima_fecha AS (
  SELECT year, MAX(round) AS ultima_ronda
  FROM f1_curated.races
  GROUP BY year
)
SELECT ra.year         AS temporada,
       c.name          AS constructor,
       cs.points       AS puntos,
       cs.wins         AS victorias
FROM f1_curated.constructor_standings cs
JOIN f1_curated.races ra      ON cs.race_id = ra.race_id
JOIN ultima_fecha u           ON ra.year = u.year AND ra.round = u.ultima_ronda
JOIN f1_curated.constructors c ON cs.constructor_id = c.constructor_id
WHERE cs.position = 1
ORDER BY temporada DESC;

-- name: 03 Pilotos colombianos en la F1
SELECT d.forename || ' ' || d.surname                  AS piloto,
       COUNT(*)                                        AS carreras,
       SUM(CASE WHEN r.position = 1 THEN 1 ELSE 0 END) AS victorias,
       SUM(CASE WHEN r.position <= 3 THEN 1 ELSE 0 END) AS podios,
       SUM(r.points)                                   AS puntos
FROM f1_curated.results r
JOIN f1_curated.drivers d ON r.driver_id = d.driver_id
WHERE d.nationality = 'Colombian'
GROUP BY 1
ORDER BY carreras DESC;

-- name: 04 ¿Cuánto vale la pole? Victorias desde P1 por década
SELECT CAST(FLOOR(ra.year / 10.0) * 10 AS INTEGER)                                      AS decada,
       COUNT(*)                                                  AS carreras,
       SUM(CASE WHEN r.grid = 1 THEN 1 ELSE 0 END)               AS ganadas_desde_pole,
       ROUND(100.0 * SUM(CASE WHEN r.grid = 1 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct
FROM f1_curated.results r
JOIN f1_curated.races ra ON r.race_id = ra.race_id
WHERE r.position = 1
GROUP BY 1
ORDER BY decada;

-- name: 05 Tiempo en pit lane por equipo (última temporada)
WITH temporada AS (
  SELECT MAX(ra.year) AS year
  FROM f1_curated.pit_stops p
  JOIN f1_curated.races ra ON p.race_id = ra.race_id
)
SELECT c.name                             AS constructor,
       COUNT(*)                           AS paradas,
       ROUND(AVG(p.milliseconds) / 1000.0, 2) AS promedio_pitlane_seg,
       ROUND(MIN(p.milliseconds) / 1000.0, 2) AS mejor_seg
FROM f1_curated.pit_stops p
JOIN f1_curated.races ra   ON p.race_id = ra.race_id
JOIN temporada t           ON ra.year = t.year
JOIN f1_curated.results r  ON r.race_id = p.race_id AND r.driver_id = p.driver_id
JOIN f1_curated.constructors c ON r.constructor_id = c.constructor_id
WHERE p.milliseconds < 60000   -- descarta paradas largas (reparaciones, banderas rojas)
GROUP BY 1
ORDER BY promedio_pitlane_seg;

-- name: 06 Circuitos con más Grandes Premios
SELECT ci.name    AS circuito,
       ci.country AS pais,
       COUNT(*)   AS grandes_premios,
       MIN(ra.year) AS primera_vez,
       MAX(ra.year) AS ultima_vez
FROM f1_curated.races ra
JOIN f1_curated.circuits ci ON ra.circuit_id = ci.circuit_id
GROUP BY 1, 2
ORDER BY grandes_premios DESC
LIMIT 10;

-- name: 07 CSV vs Parquet: compara "Data scanned"
-- Ejecuta las dos consultas por separado y compara los bytes escaneados
-- que muestra Athena debajo del resultado. Athena cobra por bytes escaneados.
SELECT COUNT(*), SUM(CAST(points AS DOUBLE)) FROM f1_raw.results;
-- SELECT COUNT(*), SUM(points) FROM f1_curated.results;

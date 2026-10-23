-- =================================================================================
-- SCRIPT: CONSULTAS ANALÍTICAS REFINADAS E INTEGRADAS (R1 A R7) - SEGUNDA ENTREGA
-- Base de Datos: dw_calidad_agua_colombia (MySQL)
-- =================================================================================

-- ---------------------------------------------------------------------------------
-- R1 (Mantenido y Refinado): Evolución histórica nacional del IRCA (General, Urbano y Rural) desde 2007.
-- Justificación técnica: AVG() ignora valores NULL reales de forma nativa sin sesgar hacia 0.
-- ---------------------------------------------------------------------------------
SELECT 
    t.anio,
    COUNT(f.fk_geografia) AS municipios_evaluados,
    ROUND(AVG(f.irca_general), 2) AS prom_irca_general,
    ROUND(AVG(f.irca_urbano), 2) AS prom_irca_urbano,
    ROUND(AVG(f.irca_rural), 2) AS prom_irca_rural
FROM fact_irca f
JOIN dim_tiempo t ON f.fk_tiempo = t.sk_tiempo
GROUP BY t.anio
ORDER BY t.anio ASC;


-- ---------------------------------------------------------------------------------
-- R2 (Refinado por Observación del Profesor): Top 5 departamentos con peor índice de
-- calidad de agua en la actualidad (último año disponible).
-- CORRECCIÓN CRÍTICA: Se eliminó por completo la expresión errónea:
--   AVG((COALESCE(f.irca_urbano, 0) + COALESCE(f.irca_rural, 0)) / 2)
-- Porque en el dominio del IRCA, 0% representa "Agua Sin Riesgo" (óptima). Reemplazar un
-- valor faltante ('ND' -> NULL) por 0 reducía artificialmente a la mitad el riesgo de los
-- municipios sin medición rural (8,968 registros históricos afectados) y alteraba el ranking.
-- Ahora se utiliza el atributo oficial consolidado `irca_general` (100% completitud) y se
-- reportan en paralelo los promedios reales urbano y rural ignorando NULLs.
-- ---------------------------------------------------------------------------------
SELECT 
    g.departamento,
    g.region_geografica,
    COUNT(f.fk_geografia) AS municipios_medidos,
    ROUND(AVG(f.irca_general), 2) AS prom_irca_consolidado_real,
    ROUND(AVG(f.irca_urbano), 2) AS prom_irca_urbano_real,
    ROUND(AVG(f.irca_rural), 2) AS prom_irca_rural_real
FROM fact_irca f
JOIN dim_geografia g ON f.fk_geografia = g.sk_geografia
JOIN dim_tiempo t ON f.fk_tiempo = t.sk_tiempo
WHERE t.anio = (SELECT MAX(anio) FROM dim_tiempo)
GROUP BY g.departamento, g.region_geografica
ORDER BY prom_irca_consolidado_real DESC
LIMIT 5;


-- ---------------------------------------------------------------------------------
-- R3 (Refinado): Brecha de desigualdad entre zona rural y urbana por departamento.
-- Se evalúa tanto la diferencia de promedios departamentales como el promedio de la
-- brecha municipal exacta (`brecha_rural_urbana`), donde ambas zonas tienen medición real.
-- ---------------------------------------------------------------------------------
SELECT 
    g.departamento,
    ROUND(AVG(f.irca_urbano), 2) AS prom_urbano,
    ROUND(AVG(f.irca_rural), 2) AS prom_rural,
    ROUND(AVG(f.irca_rural) - AVG(f.irca_urbano), 2) AS brecha_departamental,
    ROUND(AVG(f.brecha_rural_urbana), 2) AS brecha_municipal_pareada
FROM fact_irca f
JOIN dim_geografia g ON f.fk_geografia = g.sk_geografia
GROUP BY g.departamento
HAVING prom_rural IS NOT NULL AND prom_urbano IS NOT NULL
ORDER BY brecha_departamental DESC;


-- ---------------------------------------------------------------------------------
-- R4 (Refinado): Municipios que se han mantenido en "Alto Riesgo" o "Inviable Sanitariamente"
-- (IRCA > 35%) de forma constante durante todos los últimos 5 años (lustro reciente).
-- Se evalúa tanto sobre el IRCA general como sobre el IRCA rural.
-- ---------------------------------------------------------------------------------
SELECT 
    g.codigo_municipio,
    g.departamento,
    g.municipio,
    g.region_geografica,
    COUNT(t.anio) AS anios_consecutivos_alto_riesgo,
    ROUND(AVG(f.irca_general), 2) AS prom_irca_general_5a,
    ROUND(AVG(f.irca_rural), 2) AS prom_irca_rural_5a
FROM fact_irca f
JOIN dim_geografia g ON f.fk_geografia = g.sk_geografia
JOIN dim_tiempo t ON f.fk_tiempo = t.sk_tiempo
WHERE t.es_lustro_reciente = 1
  AND (f.irca_general > 35.0 OR f.irca_rural > 35.0)
GROUP BY g.codigo_municipio, g.departamento, g.municipio, g.region_geografica
HAVING COUNT(t.anio) = 5
ORDER BY prom_irca_general_5a DESC;


-- ---------------------------------------------------------------------------------
-- R5 (Mantenido): Departamentos que han logrado la mayor reducción histórica de riesgo
-- en calidad de agua rural (comparación entre primer año base y último año actual).
-- ---------------------------------------------------------------------------------
WITH min_anio AS (
    SELECT g.departamento, AVG(f.irca_rural) AS irca_base
    FROM fact_irca f
    JOIN dim_geografia g ON f.fk_geografia = g.sk_geografia
    JOIN dim_tiempo t ON f.fk_tiempo = t.sk_tiempo
    WHERE t.anio = (SELECT MIN(anio) FROM dim_tiempo)
    GROUP BY g.departamento
),
max_anio AS (
    SELECT g.departamento, AVG(f.irca_rural) AS irca_actual
    FROM fact_irca f
    JOIN dim_geografia g ON f.fk_geografia = g.sk_geografia
    JOIN dim_tiempo t ON f.fk_tiempo = t.sk_tiempo
    WHERE t.anio = (SELECT MAX(anio) FROM dim_tiempo)
    GROUP BY g.departamento
)
SELECT 
    a.departamento,
    ROUND(a.irca_base, 2) AS riesgo_rural_base_2007,
    ROUND(b.irca_actual, 2) AS riesgo_rural_actual_2024,
    ROUND(b.irca_actual - a.irca_base, 2) AS reduccion_puntos_irca
FROM min_anio a
JOIN max_anio b ON a.departamento = b.departamento
WHERE a.irca_base IS NOT NULL 
  AND b.irca_actual IS NOT NULL
  AND (b.irca_actual - a.irca_base) < 0
ORDER BY reduccion_puntos_irca ASC;


-- ---------------------------------------------------------------------------------
-- R6 (NUEVO - Integrado CSV INS + API Colombia /Department & /Region):
-- Exposición Demográfica Departamental al Riesgo Hídrico y Densidad Poblacional en el último año.
-- Combina el IRCA municipal/departamental (Fuente Primaria) con la Población, Superficie y
-- Región Natural (API Colombia) para cuantificar cuántas personas habitan en departamentos
-- con riesgo de agua (> 5%) y qué proporción de municipios por departamento presenta riesgo crítico.
-- ---------------------------------------------------------------------------------
SELECT 
    g.region_geografica,
    g.departamento,
    MAX(g.poblacion_depto_api) AS poblacion_departamental_api,
    MAX(g.densidad_depto_hab_km2) AS densidad_hab_km2_api,
    COUNT(f.fk_geografia) AS total_municipios_evaluados,
    SUM(CASE WHEN f.irca_general > 14.0 THEN 1 ELSE 0 END) AS municipios_riesgo_medio_o_alto,
    ROUND(SUM(CASE WHEN f.irca_general > 14.0 THEN 1 ELSE 0 END) * 100.0 / COUNT(f.fk_geografia), 2) AS pct_municipios_afectados,
    ROUND(AVG(f.irca_general), 2) AS prom_irca_depto,
    -- Estimación ponderada de población departamental en jurisdicciones con riesgo medio/alto (>14%)
    ROUND(MAX(g.poblacion_depto_api) * (SUM(CASE WHEN f.irca_general > 14.0 THEN 1 ELSE 0 END) / COUNT(f.fk_geografia)), 0) AS poblacion_estimada_expuesta_riesgo
FROM fact_irca f
JOIN dim_geografia g ON f.fk_geografia = g.sk_geografia
JOIN dim_tiempo t ON f.fk_tiempo = t.sk_tiempo
WHERE t.anio = (SELECT MAX(anio) FROM dim_tiempo)
GROUP BY g.region_geografica, g.departamento
ORDER BY poblacion_estimada_expuesta_riesgo DESC;


-- ---------------------------------------------------------------------------------
-- R7 (NUEVO - Integrado CSV INS + API Colombia /IndigenousReservation):
-- Brecha de Vulnerabilidad Hídrica en Municipios con Presencia de Resguardos Indígenas.
-- Parte 7A: Comparativa nacional y regional del IRCA General y Rural entre municipios
-- CON resguardos indígenas vs. SIN resguardos indígenas (en el último año y serie histórica).
-- ---------------------------------------------------------------------------------
SELECT 
    CASE WHEN g.tiene_resguardo_indigena = 1 THEN 'Con Resguardo Indígena (API)' ELSE 'Sin Resguardo Indígena' END AS condicion_territorial,
    COUNT(DISTINCT g.sk_geografia) AS cantidad_municipios,
    SUM(g.total_resguardos_indigenas) / COUNT(DISTINCT t.anio) AS total_resguardos_asociados,
    ROUND(AVG(f.irca_general), 2) AS prom_irca_general_historico,
    ROUND(AVG(f.irca_rural), 2) AS prom_irca_rural_historico,
    ROUND(AVG(CASE WHEN t.anio = (SELECT MAX(anio) FROM dim_tiempo) THEN f.irca_general END), 2) AS prom_irca_general_2024,
    ROUND(AVG(CASE WHEN t.anio = (SELECT MAX(anio) FROM dim_tiempo) THEN f.irca_rural END), 2) AS prom_irca_rural_2024
FROM fact_irca f
JOIN dim_geografia g ON f.fk_geografia = g.sk_geografia
JOIN dim_tiempo t ON f.fk_tiempo = t.sk_tiempo
GROUP BY g.tiene_resguardo_indigena
ORDER BY g.tiene_resguardo_indigena DESC;

-- Parte 7B: Top 10 Municipios con Resguardos Indígenas en estado crítico de agua en 2024
SELECT 
    g.codigo_municipio,
    g.departamento,
    g.municipio,
    g.region_geografica,
    g.total_resguardos_indigenas,
    f.irca_general AS irca_general_2024,
    f.irca_rural AS irca_rural_2024,
    f.nivel_riesgo_general
FROM fact_irca f
JOIN dim_geografia g ON f.fk_geografia = g.sk_geografia
JOIN dim_tiempo t ON f.fk_tiempo = t.sk_tiempo
WHERE t.anio = (SELECT MAX(anio) FROM dim_tiempo)
  AND g.tiene_resguardo_indigena = 1
ORDER BY f.irca_general DESC
LIMIT 10;

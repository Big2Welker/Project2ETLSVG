-- =================================================================================
-- DDL: DATA WAREHOUSE REFINADO (SEGUNDA ENTREGA - ODS 6 CALIDAD DEL AGUA COLOMBIA)
-- Motor: MySQL 8.0+ | Base de Datos: dw_calidad_agua_colombia
-- =================================================================================

CREATE DATABASE IF NOT EXISTS dw_calidad_agua_colombia
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE dw_calidad_agua_colombia;

DROP TABLE IF EXISTS fact_irca;
DROP TABLE IF EXISTS dim_geografia;
DROP TABLE IF EXISTS dim_tiempo;
DROP TABLE IF EXISTS etl_batch_audit;

-- 1. Dimensión Temporal (Grano: Año calendario)
CREATE TABLE dim_tiempo (
    sk_tiempo INT PRIMARY KEY COMMENT 'Clave sustituta temporal',
    anio INT NOT NULL UNIQUE COMMENT 'Año de medición (Clave de negocio)',
    decada INT NOT NULL COMMENT 'Década para agregación histórica',
    es_lustro_reciente TINYINT(1) NOT NULL COMMENT '1 si pertenece a los últimos 5 años evaluados (2020-2024)'
);

-- 2. Dimensión Geográfica Conformada y Enriquecida con API Colombia (Grano: Municipio DIVIPOLA)
CREATE TABLE dim_geografia (
    sk_geografia INT PRIMARY KEY COMMENT 'Clave sustituta geográfica',
    codigo_departamento VARCHAR(5) NOT NULL COMMENT 'Código DANE DIVIPOLA Departamento (2 dígitos)',
    departamento VARCHAR(150) NOT NULL COMMENT 'Nombre oficial del departamento estandarizado',
    codigo_municipio VARCHAR(10) NOT NULL UNIQUE COMMENT 'Código DANE DIVIPOLA Municipio (5 dígitos - Business Key)',
    municipio VARCHAR(150) NOT NULL COMMENT 'Nombre oficial del municipio estandarizado',
    region_geografica VARCHAR(100) NOT NULL COMMENT 'Región natural de Colombia obtenida vía API Colombia (/Region)',
    poblacion_depto_api BIGINT NOT NULL COMMENT 'Población departamental extraída de API Colombia (/Department)',
    superficie_depto_km2_api DECIMAL(12,2) NOT NULL COMMENT 'Superficie departamental en km2 extraída de API Colombia',
    densidad_depto_hab_km2 DECIMAL(12,2) NOT NULL COMMENT 'Densidad demográfica derivada (hab/km2)',
    total_resguardos_indigenas INT NOT NULL DEFAULT 0 COMMENT 'Conteo de resguardos indígenas en el municipio vía API Colombia (/IndigenousReservation)',
    tiene_resguardo_indigena TINYINT(1) NOT NULL DEFAULT 0 COMMENT 'Indicador 1/0 de presencia de resguardos indígenas en el municipio'
);

-- 3. Tabla de Hechos Fact_IRCA (Grano: Medición anual consolidada de IRCA por municipio y año)
CREATE TABLE fact_irca (
    fk_geografia INT NOT NULL COMMENT 'FK hacia dim_geografia',
    fk_tiempo INT NOT NULL COMMENT 'FK hacia dim_tiempo',
    irca_general DECIMAL(6,2) NOT NULL COMMENT 'IRCA consolidado municipal oficial INS (0.00 a 100.00)',
    irca_urbano DECIMAL(6,2) NULL COMMENT 'IRCA zona urbana (NULL cuando no existe medición real; NUNCA se imputa 0)',
    irca_rural DECIMAL(6,2) NULL COMMENT 'IRCA zona rural (NULL cuando no existe medición real; NUNCA se imputa 0)',
    brecha_rural_urbana DECIMAL(6,2) NULL COMMENT 'Diferencia (irca_rural - irca_urbano) calculada solo cuando ambas zonas tienen dato real',
    nivel_riesgo_general VARCHAR(50) NOT NULL COMMENT 'Clasificación normativa Res. 2115/2007 según irca_general',
    PRIMARY KEY (fk_geografia, fk_tiempo),
    CONSTRAINT fk_fact_geo FOREIGN KEY (fk_geografia) REFERENCES dim_geografia(sk_geografia),
    CONSTRAINT fk_fact_tiempo FOREIGN KEY (fk_tiempo) REFERENCES dim_tiempo(sk_tiempo)
);

-- 4. Tabla de Auditoría y Reconciliación de Lotes (Observabilidad y Safe Rerun)
CREATE TABLE etl_batch_audit (
    batch_id VARCHAR(100) PRIMARY KEY,
    loaded_at_utc VARCHAR(60) NOT NULL,
    dim_tiempo_count INT NOT NULL,
    dim_geografia_count INT NOT NULL,
    fact_irca_count INT NOT NULL,
    avg_irca_general DECIMAL(8,4) NOT NULL
);

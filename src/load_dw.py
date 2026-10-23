"""
Módulo de Carga Idempotente del Data Warehouse en MySQL y Reconciliación Post-Carga (Sección 15 y 20.3).
Implementa:
- Creación automática de la base de datos si no existe.
- DDL estricto con claves primarias (PK), claves de negocio únicas (UNIQUE) y claves foráneas (FK).
- Estrategia de Reejecución Segura (Safe Rerun / Idempotencia):
  Registra conteos pre-carga en el DW, ejecuta reemplazo transaccional controlado
  (TRUNCATE/DELETE en orden inverso de FK + inserción validada) y verifica la reconciliación
  exacta post-carga (conteos y promedios de control).
"""

import os
import json
from datetime import datetime, timezone
from typing import Dict, Any
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS dim_tiempo (
        sk_tiempo INT PRIMARY KEY,
        anio INT NOT NULL UNIQUE,
        decada INT NOT NULL,
        es_lustro_reciente TINYINT(1) NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_geografia (
        sk_geografia INT PRIMARY KEY,
        codigo_departamento VARCHAR(5) NOT NULL,
        departamento VARCHAR(150) NOT NULL,
        codigo_municipio VARCHAR(10) NOT NULL UNIQUE,
        municipio VARCHAR(150) NOT NULL,
        region_geografica VARCHAR(100) NOT NULL,
        poblacion_depto_api BIGINT NOT NULL,
        superficie_depto_km2_api DECIMAL(12,2) NOT NULL,
        densidad_depto_hab_km2 DECIMAL(12,2) NOT NULL,
        total_resguardos_indigenas INT NOT NULL DEFAULT 0,
        tiene_resguardo_indigena TINYINT(1) NOT NULL DEFAULT 0
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_irca (
        fk_geografia INT NOT NULL,
        fk_tiempo INT NOT NULL,
        irca_general DECIMAL(6,2) NOT NULL,
        irca_urbano DECIMAL(6,2) NULL,
        irca_rural DECIMAL(6,2) NULL,
        brecha_rural_urbana DECIMAL(6,2) NULL,
        nivel_riesgo_general VARCHAR(50) NOT NULL,
        PRIMARY KEY (fk_geografia, fk_tiempo),
        CONSTRAINT fk_fact_geo FOREIGN KEY (fk_geografia) REFERENCES dim_geografia(sk_geografia),
        CONSTRAINT fk_fact_tiempo FOREIGN KEY (fk_tiempo) REFERENCES dim_tiempo(sk_tiempo)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS etl_batch_audit (
        batch_id VARCHAR(100) PRIMARY KEY,
        loaded_at_utc VARCHAR(60) NOT NULL,
        dim_tiempo_count INT NOT NULL,
        dim_geografia_count INT NOT NULL,
        fact_irca_count INT NOT NULL,
        avg_irca_general DECIMAL(8,4) NOT NULL
    );
    """
]


def get_db_uris() -> Dict[str, str]:
    env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".env"))
    if os.path.exists(env_path):
        load_dotenv(env_path, override=False)
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "root")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "3306")
    db_name = os.getenv("DB_NAME", "dw_calidad_agua_colombia")

    if not user or password is None:
        raise ValueError("Credenciales DB_USER o DB_PASSWORD faltantes en .env")

    server_uri = f"mysql+pymysql://{user}:{password}@{host}:{port}/"
    db_uri = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}"
    return {"server_uri": server_uri, "db_uri": db_uri, "db_name": db_name}


def load_dimensional_dw(prepared_dir: str, evidence_dir: str, batch_id: str) -> Dict[str, Any]:
    """
    Carga los datos preparados y validados en el Data Warehouse MySQL de manera idempotente.
    Genera evidencia de reconciliación antes y después de la carga (soporta Test A y Test C Safe Rerun).
    """
    os.makedirs(evidence_dir, exist_ok=True)
    uris = get_db_uris()
    db_name = uris["db_name"]

    print(f"[LOAD DW] Asegurando existencia de la base de datos '{db_name}'...")
    server_engine = create_engine(uris["server_uri"], isolation_level="AUTOCOMMIT")
    with server_engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))

    engine = create_engine(uris["db_uri"])

    dim_tiempo = pd.read_csv(os.path.join(prepared_dir, "dim_tiempo.csv"))
    dim_geo = pd.read_csv(os.path.join(prepared_dir, "dim_geografia.csv"))
    fact_irca = pd.read_csv(os.path.join(prepared_dir, "fact_irca.csv"))

    # Convertir NaN de Pandas a None nativo para que MySQL inserte NULL real (y NUNCA cero)
    fact_irca_sql = fact_irca.replace({np.nan: None})

    with engine.begin() as conn:
        # Si existe el esquema viejo de la Entrega 1 sin las columnas nuevas, recrear estructura limpia
        inspector_cols = conn.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema = :db AND table_name = 'dim_geografia' AND column_name = 'codigo_municipio'"
            ),
            {"db": db_name}
        ).scalar()

        if inspector_cols == 0:
            print("[LOAD DW] Actualizando esquema dimensional de Entrega 1 a Modelo Estrella Refinado (Entrega 2)...")
            conn.execute(text("DROP TABLE IF EXISTS fact_irca;"))
            conn.execute(text("DROP TABLE IF EXISTS dim_geografia;"))
            conn.execute(text("DROP TABLE IF EXISTS dim_tiempo;"))

        for stmt in DDL_STATEMENTS:
            conn.execute(text(stmt.strip()))

        # Capturar estado ANTES de la carga (para evidencia de Safe Rerun / Reconciliación)
        pre_counts = {
            "dim_tiempo": int(conn.execute(text("SELECT COUNT(*) FROM dim_tiempo")).scalar()),
            "dim_geografia": int(conn.execute(text("SELECT COUNT(*) FROM dim_geografia")).scalar()),
            "fact_irca": int(conn.execute(text("SELECT COUNT(*) FROM fact_irca")).scalar()),
        }

        # Estrategia Idempotente (Safe Rerun): Limpiar tablas en orden hijo -> padre dentro de la misma transacción
        # Si algo falla durante la inserción, la transacción hace ROLLBACK y no corrompe el DW.
        conn.execute(text("DELETE FROM fact_irca;"))
        conn.execute(text("DELETE FROM dim_geografia;"))
        conn.execute(text("DELETE FROM dim_tiempo;"))

        # Insertar dimensiones primero (integridad referencial) y luego tabla de hechos
        dim_tiempo.to_sql("dim_tiempo", conn, if_exists="append", index=False)
        dim_geo.to_sql("dim_geografia", conn, if_exists="append", index=False)
        fact_irca_sql.to_sql("fact_irca", conn, if_exists="append", index=False, chunksize=2000)

        # Capturar estado DESPUÉS de la carga y métricas de control
        post_counts = {
            "dim_tiempo": int(conn.execute(text("SELECT COUNT(*) FROM dim_tiempo")).scalar()),
            "dim_geografia": int(conn.execute(text("SELECT COUNT(*) FROM dim_geografia")).scalar()),
            "fact_irca": int(conn.execute(text("SELECT COUNT(*) FROM fact_irca")).scalar()),
        }
        metrics_check = conn.execute(
            text(
                "SELECT "
                "ROUND(AVG(irca_general), 4) AS avg_general, "
                "ROUND(AVG(irca_urbano), 4) AS avg_urbano, "
                "ROUND(AVG(irca_rural), 4) AS avg_rural, "
                "SUM(CASE WHEN irca_rural IS NULL THEN 1 ELSE 0 END) AS null_rural_count "
                "FROM fact_irca"
            )
        ).mappings().first()

        loaded_ts = datetime.now(timezone.utc).isoformat()
        conn.execute(
            text(
                "REPLACE INTO etl_batch_audit "
                "(batch_id, loaded_at_utc, dim_tiempo_count, dim_geografia_count, fact_irca_count, avg_irca_general) "
                "VALUES (:b, :ts, :dt, :dg, :fi, :avg_g)"
            ),
            {
                "b": batch_id,
                "ts": loaded_ts,
                "dt": post_counts["dim_tiempo"],
                "dg": post_counts["dim_geografia"],
                "fi": post_counts["fact_irca"],
                "avg_g": float(metrics_check["avg_general"])
            }
        )

    # Verificación estricta de reconciliación post-carga
    assert post_counts["dim_tiempo"] == len(dim_tiempo), "Discrepancia en conteo post-carga de dim_tiempo"
    assert post_counts["dim_geografia"] == len(dim_geo), "Discrepancia en conteo post-carga de dim_geografia"
    assert post_counts["fact_irca"] == len(fact_irca), "Discrepancia en conteo post-carga de fact_irca"

    load_report = {
        "batch_id": batch_id,
        "loaded_at_utc": loaded_ts,
        "database": db_name,
        "rerun_strategy": "Transactional Delete-and-Reload in FK-safe order with Primary/Unique Key enforcement",
        "prepared_input_counts": {
            "dim_tiempo": len(dim_tiempo),
            "dim_geografia": len(dim_geo),
            "fact_irca": len(fact_irca)
        },
        "dw_pre_load_counts": pre_counts,
        "dw_post_load_counts": post_counts,
        "dw_control_metrics": {
            "avg_irca_general": float(metrics_check["avg_general"]),
            "avg_irca_urbano_ignoring_nulls": float(metrics_check["avg_urbano"]),
            "avg_irca_rural_ignoring_nulls": float(metrics_check["avg_rural"]),
            "preserved_true_nulls_in_irca_rural": int(metrics_check["null_rural_count"])
        },
        "reconciliation_passed": True
    }

    out_path = os.path.join(evidence_dir, f"dw_load_reconciliation_{batch_id}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(load_report, f, ensure_ascii=False, indent=2)

    print(f"[LOAD DW] -> Carga exitosa y reconciliada en '{db_name}'. Reporte: {out_path}")
    return load_report


if __name__ == "__main__":
    base = "c:/ETL/ProyectosEtl/Proyecto2"
    load_dimensional_dw(f"{base}/data/prepared", f"{base}/docs/evidence", "manual_load_1")

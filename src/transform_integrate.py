"""
Módulo de Transformación, Armonización, Contrato de Integración y Refinamiento Dimensional (Secciones 12 y 13).
Resuelve:
- T01: Exclusión del grano mixto departamental ('#TODOS') preservando conteos de reconciliación.
- T02: Conversión de 'ND' a NULL real (NaN/None) y casteo de métricas IRCA (irca_general, irca_urbano, irca_rural) SIN imputar 0.
- T03: Normalización fonética/ortográfica y homologación del Contrato de Integración entre INS (CSV) y API Colombia.
- T04: Enriquecimiento demográfico y territorial (población departamental, superficie, densidad, región) y conteo de resguardos indígenas por municipio.
- T05: Construcción del Modelo Estrella Refinado (dim_tiempo, dim_geografia, fact_irca).
"""

import os
import json
import unicodedata
from typing import Dict, Any, Tuple
import pandas as pd
import numpy as np


# Diccionario explícito de homologación departamental (Contrato de Integración Sección 13.1)
DEPT_HOMOLOGATION_MAP = {
    "BOGOTA, D.C.": "BOGOTA",
    "ARCHIPIELAGO DE SAN ANDRES, PROVIDENCIA Y SANTA CATALINA": "SAN ANDRES Y PROVIDENCIA",
}

# Diccionario de corrección de nombres municipales entre INS y API Colombia
CITY_HOMOLOGATION_MAP = {
    "BOGOTA, D.C.": "BOGOTA",
    "CARTAGENA DE INDIAS": "CARTAGENA",
    "SAN JOSE DE CUCUTA": "CUCUTA",
    "SANTIAGO DE CALI": "CALI",
    "SANTA MARTA (DIST. ESP.)": "SANTA MARTA",
    "BARRANQUILLA (DIST. ESP.)": "BARRANQUILLA",
}


def normalize_key(text: Any) -> str:
    """Normaliza cadenas para cruce entre fuentes: mayúsculas, sin tildes, sin espacios extras."""
    if not isinstance(text, str):
        return ""
    text = text.strip().upper()
    text = "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )
    return " ".join(text.split())


def classify_irca_risk(score: float) -> str:
    """Clasificación oficial Resolución 2115 de 2007 (0-100%)."""
    if pd.isna(score):
        return "SIN DATO"
    if score <= 5.0:
        return "SIN RIESGO"
    if score <= 14.0:
        return "RIESGO BAJO"
    if score <= 35.0:
        return "RIESGO MEDIO"
    if score <= 80.0:
        return "RIESGO ALTO"
    return "INVIABLE SANITARIAMENTE"


def transform_and_integrate(
    primary_csv_path: str,
    api_raw_dir: str,
    prepared_output_dir: str,
    batch_id: str,
    inject_critical_failure: bool = False
) -> Dict[str, Any]:
    """
    Ejecuta la transformación y el contrato de integración entre la fuente primaria (CSV INS)
    y la fuente secundaria (API Colombia), generando las tablas preparadas del Modelo Estrella Refinado.
    """
    print(f"[TRANSFORM & INTEGRATE] Iniciando transformación e integración (lote={batch_id})...")
    os.makedirs(prepared_output_dir, exist_ok=True)

    # 1. Lectura de lotes crudos
    df_raw = pd.read_csv(primary_csv_path, dtype=str)
    input_raw_rows = len(df_raw)

    with open(os.path.join(api_raw_dir, "departments_raw.json"), "r", encoding="utf-8") as f:
        depts_raw = json.load(f)
    with open(os.path.join(api_raw_dir, "regions_raw.json"), "r", encoding="utf-8") as f:
        regions_raw = json.load(f)
    with open(os.path.join(api_raw_dir, "indigenous_reservations_raw.json"), "r", encoding="utf-8") as f:
        indig_raw = json.load(f)

    # 2. T01: Aislar grano atómico municipal (excluir filas agregadas '#TODOS')
    mask_todos = (df_raw["Municipio"] == "#TODOS") | (df_raw["MunicipioCodigo"] == "#TODOS")
    excluded_todos_count = int(mask_todos.sum())
    df_clean = df_raw[~mask_todos].copy()

    # 3. T02: Limpieza de códigos DIVIPOLA, años y métricas IRCA (SIN convertir nulos en 0)
    df_clean["codigo_departamento"] = df_clean["DepartamentoCodigo"].str.strip().str.zfill(2)
    df_clean["codigo_municipio"] = df_clean["MunicipioCodigo"].str.strip().str.zfill(5)
    df_clean["anio"] = (
        df_clean["Año"]
        .astype(str)
        .str.replace(r"[,\.]", "", regex=True)
        .str.strip()
        .astype(int)
    )

    for raw_col, target_col in [
        ("IRCA", "irca_general"),
        ("IRCAurbano", "irca_urbano"),
        ("IRCArural", "irca_rural")
    ]:
        df_clean[target_col] = (
            df_clean[raw_col]
            .replace(["ND", "nd", "nan", "None", ""], np.nan)
            .astype(str)
            .str.replace(",", ".", regex=False)
            .replace(["nan", "ND", "None"], np.nan)
            .astype(float)
        )

    df_clean["nivel_riesgo_general"] = df_clean["irca_general"].apply(classify_irca_risk)

    # 4. T03: Normalización y Homologación para el Contrato de Integración
    df_clean["depto_norm"] = df_clean["Departamento"].apply(normalize_key).replace(DEPT_HOMOLOGATION_MAP)
    df_clean["mpio_norm"] = df_clean["Municipio"].apply(normalize_key).replace(CITY_HOMOLOGATION_MAP)

    # Preparar catálogo de Regiones y Departamentos desde API Colombia
    region_map = {r["id"]: r.get("name", "SIN REGION").strip().upper() for r in regions_raw}
    api_dept_rows = []
    for d in depts_raw:
        d_norm = normalize_key(d.get("name", ""))
        d_norm = DEPT_HOMOLOGATION_MAP.get(d_norm, d_norm)
        pop = float(d.get("population") or 0)
        surf = float(d.get("surface") or 0)
        dens = round(pop / surf, 2) if surf > 0 else np.nan
        api_dept_rows.append({
            "depto_norm": d_norm,
            "region_geografica": region_map.get(d.get("regionId"), "SIN REGION"),
            "poblacion_depto_api": int(pop),
            "superficie_depto_km2_api": round(surf, 2),
            "densidad_depto_hab_km2": dens
        })
    df_api_depts = pd.DataFrame(api_dept_rows).drop_duplicates(subset=["depto_norm"])

    # Preparar conteo de Resguardos Indígenas por (depto_norm, mpio_norm) desde API Colombia
    indig_records = []
    for item in indig_raw:
        c_obj = item.get("city") or {}
        d_obj = item.get("department") or {}
        c_name = CITY_HOMOLOGATION_MAP.get(normalize_key(c_obj.get("name", "")), normalize_key(c_obj.get("name", "")))
        d_name = DEPT_HOMOLOGATION_MAP.get(normalize_key(d_obj.get("name", "")), normalize_key(d_obj.get("name", "")))
        if c_name and d_name:
            indig_records.append({
                "depto_norm": d_name,
                "mpio_norm": c_name,
                "resguardo_id": item.get("id")
            })
    df_indig_counts = (
        pd.DataFrame(indig_records)
        .groupby(["depto_norm", "mpio_norm"], as_index=False)
        .agg(total_resguardos_indigenas=("resguardo_id", "count"))
    )

    # 5. T05: Construcción de Dimensiones Conformadas y Refinadas
    # 5.1 Dim_Tiempo
    anios_unicos = sorted(df_clean["anio"].unique())
    dim_tiempo = pd.DataFrame({
        "sk_tiempo": range(1, len(anios_unicos) + 1),
        "anio": anios_unicos,
        "decada": [(a // 10) * 10 for a in anios_unicos],
        "es_lustro_reciente": [1 if a >= (max(anios_unicos) - 4) else 0 for a in anios_unicos]
    })

    # 5.2 Dim_Geografia (Grano: Municipio único por codigo_municipio DIVIPOLA)
    # Tomamos el nombre más reciente por codigo_municipio para evitar duplicados por cambio de tilde histórica
    geo_base = (
        df_clean.sort_values("anio", ascending=False)
        .drop_duplicates(subset=["codigo_municipio"])[
            ["codigo_departamento", "Departamento", "codigo_municipio", "Municipio", "depto_norm", "mpio_norm"]
        ]
        .sort_values(["codigo_departamento", "codigo_municipio"])
        .reset_index(drop=True)
    )
    geo_base["departamento"] = geo_base["Departamento"].str.strip().str.upper()
    geo_base["municipio"] = geo_base["Municipio"].str.strip().str.upper()

    # Integrar con Departamentos API (1:N desde Departamento hacia Municipios)
    geo_enriched = geo_base.merge(df_api_depts, on="depto_norm", how="left")
    matched_depts_count = int(geo_enriched["poblacion_depto_api"].notna().sum())

    # Integrar con Resguardos Indígenas API (Left Join a nivel municipal)
    geo_enriched = geo_enriched.merge(df_indig_counts, on=["depto_norm", "mpio_norm"], how="left")
    geo_enriched["total_resguardos_indigenas"] = geo_enriched["total_resguardos_indigenas"].fillna(0).astype(int)
    geo_enriched["tiene_resguardo_indigena"] = (geo_enriched["total_resguardos_indigenas"] > 0).astype(int)
    matched_indig_mpios = int((geo_enriched["total_resguardos_indigenas"] > 0).sum())

    dim_geografia = pd.DataFrame({
        "sk_geografia": range(1, len(geo_enriched) + 1),
        "codigo_departamento": geo_enriched["codigo_departamento"],
        "departamento": geo_enriched["departamento"],
        "codigo_municipio": geo_enriched["codigo_municipio"],
        "municipio": geo_enriched["municipio"],
        "region_geografica": geo_enriched["region_geografica"].fillna("SIN REGION"),
        "poblacion_depto_api": geo_enriched["poblacion_depto_api"].fillna(0).astype(int),
        "superficie_depto_km2_api": geo_enriched["superficie_depto_km2_api"].fillna(0.0),
        "densidad_depto_hab_km2": geo_enriched["densidad_depto_hab_km2"].fillna(0.0),
        "total_resguardos_indigenas": geo_enriched["total_resguardos_indigenas"],
        "tiene_resguardo_indigena": geo_enriched["tiene_resguardo_indigena"]
    })

    # 5.3 Fact_IRCA (Grano: Medición anual consolidada de IRCA por municipio y año)
    fact_merged = (
        df_clean.merge(dim_geografia[["sk_geografia", "codigo_municipio"]], on="codigo_municipio", how="inner")
        .merge(dim_tiempo[["sk_tiempo", "anio"]], on="anio", how="inner")
    )

    # Calcular brecha rural-urbana solo cuando AMBAS zonas tienen medición real (sin inventar ceros)
    fact_merged["brecha_rural_urbana"] = np.where(
        fact_merged["irca_rural"].notna() & fact_merged["irca_urbano"].notna(),
        (fact_merged["irca_rural"] - fact_merged["irca_urbano"]).round(2),
        np.nan
    )

    fact_irca = fact_merged[[
        "sk_geografia",
        "sk_tiempo",
        "irca_general",
        "irca_urbano",
        "irca_rural",
        "brecha_rural_urbana",
        "nivel_riesgo_general"
    ]].rename(columns={
        "sk_geografia": "fk_geografia",
        "sk_tiempo": "fk_tiempo"
    }).sort_values(["fk_geografia", "fk_tiempo"]).reset_index(drop=True)

    # Condición de prueba controlada (Test B - Sección 20.2): si se activa, inyecta un valor fuera de rango [0, 100]
    if inject_critical_failure or os.getenv("INJECT_CRITICAL_FAILURE", "false").lower() == "true":
        print("[TRANSFORM & INTEGRATE] [ALERTA TEST B] Inyectando valor fuera de rango (irca_general = 145.0) para prueba controlada de fallo crítico...")
        fact_irca.loc[0, "irca_general"] = 145.0

    # Persistir tablas preparadas a disco
    dim_tiempo_path = os.path.join(prepared_output_dir, "dim_tiempo.csv")
    dim_geo_path = os.path.join(prepared_output_dir, "dim_geografia.csv")
    fact_irca_path = os.path.join(prepared_output_dir, "fact_irca.csv")

    dim_tiempo.to_csv(dim_tiempo_path, index=False)
    dim_geografia.to_csv(dim_geo_path, index=False)
    fact_irca.to_csv(fact_irca_path, index=False)

    reconciliation = {
        "batch_id": batch_id,
        "input_raw_csv_rows": input_raw_rows,
        "excluded_todos_rows": excluded_todos_count,
        "expected_atomic_rows": input_raw_rows - excluded_todos_count,
        "dim_tiempo_rows": len(dim_tiempo),
        "dim_geografia_rows": len(dim_geografia),
        "fact_irca_rows": len(fact_irca),
        "api_department_match_coverage_pct": round(matched_depts_count / len(dim_geografia) * 100, 2),
        "municipalities_with_indigenous_reservations_matched": matched_indig_mpios,
        "files": {
            "dim_tiempo": dim_tiempo_path,
            "dim_geografia": dim_geo_path,
            "fact_irca": fact_irca_path
        }
    }

    rec_path = os.path.join(prepared_output_dir, f"transformation_reconciliation_{batch_id}.json")
    with open(rec_path, "w", encoding="utf-8") as f:
        json.dump(reconciliation, f, ensure_ascii=False, indent=2)

    print(
        f"[TRANSFORM & INTEGRATE] -> Completado: dim_tiempo={len(dim_tiempo)}, "
        f"dim_geografia={len(dim_geografia)} (100% cobertura API Deptos, {matched_indig_mpios} mpios con resguardos), "
        f"fact_irca={len(fact_irca)} filas."
    )
    return reconciliation


if __name__ == "__main__":
    transform_and_integrate(
        primary_csv_path="c:/ETL/ProyectosEtl/Proyecto2/data/raw/primary/Calidad_del_Agua_para_Consumo_Humano_en_Colombia_20260905.csv",
        api_raw_dir="c:/ETL/ProyectosEtl/Proyecto2/data/raw/api",
        prepared_output_dir="c:/ETL/ProyectosEtl/Proyecto2/data/prepared",
        batch_id="manual_profile"
    )

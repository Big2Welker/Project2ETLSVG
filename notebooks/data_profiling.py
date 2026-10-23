"""
Script Reproducible de Data Profiling (Sección 7 - Segunda Entrega).
IMPORTANTE: Este script OBSERVA y PRODUCE EVIDENCIA sobre las fuentes crudas
(CSV Primario del INS y JSON Crudo de API Colombia) sin modificar ni limpiar
silenciosamente los datos de origen.
"""

import os
import json
import unicodedata
import pandas as pd
import numpy as np


def _normalize_text(val: str) -> str:
    if not isinstance(val, str):
        return ""
    val = val.strip().upper()
    val = "".join(
        c for c in unicodedata.normalize("NFD", val)
        if unicodedata.category(c) != "Mn"
    )
    return val


def run_profiling(
    primary_csv_path: str,
    api_raw_dir: str,
    evidence_output_dir: str
):
    os.makedirs(evidence_output_dir, exist_ok=True)
    df_raw = pd.read_csv(primary_csv_path, dtype=str)

    with open(os.path.join(api_raw_dir, "departments_raw.json"), "r", encoding="utf-8") as f:
        depts_raw = json.load(f)
    with open(os.path.join(api_raw_dir, "regions_raw.json"), "r", encoding="utf-8") as f:
        regions_raw = json.load(f)
    with open(os.path.join(api_raw_dir, "cities_raw.json"), "r", encoding="utf-8") as f:
        cities_raw = json.load(f)
    with open(os.path.join(api_raw_dir, "indigenous_reservations_raw.json"), "r", encoding="utf-8") as f:
        indig_raw = json.load(f)

    # 1. Perfilado Fuente Primaria (CSV INS)
    total_csv_rows = len(df_raw)
    todos_rows = int((df_raw["Municipio"] == "#TODOS").sum())
    atomic_df = df_raw[df_raw["Municipio"] != "#TODOS"].copy()
    atomic_rows = len(atomic_df)

    nd_counts = {
        col: int((atomic_df[col] == "ND").sum())
        for col in ["IRCA", "IRCAurbano", "IRCArural"]
    }
    nd_pcts = {
        col: round(nd_counts[col] / atomic_rows * 100, 2)
        for col in ["IRCA", "IRCAurbano", "IRCArural"]
    }

    # Verificar cuántos municipios tienen IRCAurbano válido pero IRCArural = 'ND' (o viceversa)
    # Esto demuestra numéricamente la observación del profesor sobre R2 (COALESCE a 0)
    one_zone_nd = int(
        (
            ((atomic_df["IRCAurbano"] != "ND") & (atomic_df["IRCArural"] == "ND")) |
            ((atomic_df["IRCAurbano"] == "ND") & (atomic_df["IRCArural"] != "ND"))
        ).sum()
    )

    # Verificar unicidad del grano candidato (MunicipioCodigo, Año) en filas atómicas
    dup_grain_code_year = int(atomic_df.duplicated(subset=["MunicipioCodigo", "Año"]).sum())
    dup_grain_name_year = int(atomic_df.duplicated(subset=["Departamento", "Municipio", "Año"]).sum())

    # Rangos numéricos observados (sin modificar el original)
    irca_num = pd.to_numeric(atomic_df["IRCA"].str.replace(",", "."), errors="coerce")
    irca_u_num = pd.to_numeric(atomic_df["IRCAurbano"].str.replace(",", "."), errors="coerce")
    irca_r_num = pd.to_numeric(atomic_df["IRCArural"].str.replace(",", "."), errors="coerce")
    anios_num = pd.to_numeric(atomic_df["Año"].str.replace(r"[,\.]", "", regex=True), errors="coerce")

    # 2. Perfilado Fuente Secundaria (API Colombia)
    df_depts = pd.DataFrame(depts_raw)
    df_cities = pd.DataFrame(cities_raw)
    df_indig = pd.DataFrame(indig_raw)

    dept_pop_nulls = int(df_depts["population"].isna().sum())
    dept_surf_nulls = int(df_depts["surface"].isna().sum())

    city_pop_non_null = int(df_cities["population"].notna().sum())
    city_pop_null_pct = round((1 - city_pop_non_null / len(df_cities)) * 100, 2)

    # Esquema de IndigenousReservation: detectar el typo 'deparmentId' vs 'departmentId'
    indig_keys = list(df_indig.columns)
    indig_city_nulls = int(df_indig["cityId"].isna().sum())

    # 3. Compatibilidad Cruzada (Cross-Source Integration Profiling)
    csv_depts_raw = set(atomic_df["Departamento"].dropna().unique())
    api_depts_raw = set(df_depts["name"].dropna().unique())
    exact_dept_matches = len(csv_depts_raw.intersection(api_depts_raw))

    csv_depts_norm = {_normalize_text(x) for x in csv_depts_raw}
    api_depts_norm = {_normalize_text(x) for x in api_depts_raw}
    norm_dept_matches = len(csv_depts_norm.intersection(api_depts_norm))
    unmatched_csv_depts = sorted(list(csv_depts_norm - api_depts_norm))
    unmatched_api_depts = sorted(list(api_depts_norm - csv_depts_norm))

    # Cruce de municipios con resguardos indígenas
    # En IndigenousReservation cada registro tiene un objeto anidado 'city' y 'department'
    indig_cities_names = set()
    for item in indig_raw:
        c_obj = item.get("city")
        d_obj = item.get("department")
        if isinstance(c_obj, dict) and c_obj.get("name"):
            d_name = d_obj.get("name", "") if isinstance(d_obj, dict) else ""
            indig_cities_names.add((_normalize_text(d_name), _normalize_text(c_obj.get("name", ""))))

    summary = {
        "primary_source_csv": {
            "total_records": total_csv_rows,
            "aggregated_todos_records": todos_rows,
            "atomic_municipality_records": atomic_rows,
            "columns": list(df_raw.columns),
            "year_min": int(anios_num.min()),
            "year_max": int(anios_num.max()),
            "nd_counts_in_atomic": nd_counts,
            "nd_percentages_in_atomic": nd_pcts,
            "single_zone_nd_records_affected_by_zero_coalesce": one_zone_nd,
            "duplicate_candidate_key_municipio_codigo_anio": dup_grain_code_year,
            "duplicate_candidate_key_depto_mpio_anio": dup_grain_name_year,
            "irca_general_range": [float(irca_num.min()), float(irca_num.max())],
            "irca_urbano_range": [float(irca_u_num.min()), float(irca_u_num.max())],
            "irca_rural_range": [float(irca_r_num.min()), float(irca_r_num.max())]
        },
        "api_source_colombia": {
            "departments_count": len(df_depts),
            "regions_count": len(regions_raw),
            "departments_population_nulls": dept_pop_nulls,
            "departments_surface_nulls": dept_surf_nulls,
            "cities_total_count": len(df_cities),
            "cities_with_population_count": city_pop_non_null,
            "cities_population_null_percentage": city_pop_null_pct,
            "indigenous_reservations_count": len(df_indig),
            "indigenous_reservations_columns": indig_keys,
            "indigenous_reservations_missing_cityId": indig_city_nulls,
            "unique_municipalities_with_indigenous_reservations": len(indig_cities_names)
        },
        "cross_source_compatibility": {
            "csv_unique_departments": len(csv_depts_raw),
            "api_unique_departments": len(api_depts_raw),
            "exact_string_department_matches": exact_dept_matches,
            "normalized_string_department_matches": norm_dept_matches,
            "unmatched_csv_departments_before_mapping": unmatched_csv_depts,
            "unmatched_api_departments_before_mapping": unmatched_api_depts
        }
    }

    json_out = os.path.join(evidence_output_dir, "profiling_summary.json")
    with open(json_out, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return summary


if __name__ == "__main__":
    run_profiling(
        primary_csv_path="c:/ETL/ProyectosEtl/Proyecto2/data/raw/primary/Calidad_del_Agua_para_Consumo_Humano_en_Colombia_20260905.csv",
        api_raw_dir="c:/ETL/ProyectosEtl/Proyecto2/data/raw/api",
        evidence_output_dir="c:/ETL/ProyectosEtl/Proyecto2/docs/evidence"
    )

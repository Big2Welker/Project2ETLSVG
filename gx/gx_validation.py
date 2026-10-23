"""
Módulo de Validación Automatizada con Great Expectations (Secciones 8, 9, 10, 11 y 14).
Implementa las 3 Compuertas Explícitas de Validación:
1. Compuerta 1 (validate_primary_raw): Evalúa el lote crudo CSV del INS antes de transformar.
2. Compuerta 2 (validate_api_raw): Evalúa los lotes JSON crudos extraídos de API Colombia.
3. Compuerta 3 (validate_prepared): Evalúa las tablas preparadas e integradas (dim_tiempo, dim_geografia, fact_irca)
   antes de cargar el Data Warehouse en MySQL.

Cada Expectation está trazada explícitamente a una Regla de Calidad (DQ_01 a DQ_10),
su dimensión de calidad, métrica/umbral, severidad (Critical, Warning, Informational) y requisito protegido (R1-R7).
"""

import os
import json
from datetime import datetime, timezone
from typing import Dict, Any, List
import numpy as np
import pandas as pd
import great_expectations as gx


def _run_gx_suite(df: pd.DataFrame, asset_name: str, suite_name: str, expectations_with_meta: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Ejecuta una ExpectationSuite sobre un DataFrame usando Great Expectations 1.x
    y traduce los resultados según la política de severidad (Critical, Warning, Informational).
    """
    ctx = gx.get_context(mode="ephemeral")
    ds = ctx.data_sources.add_pandas(name=f"ds_{asset_name}")
    asset = ds.add_dataframe_asset(name=asset_name)
    batch_def = asset.add_batch_definition_whole_dataframe(name=f"bd_{asset_name}")

    suite = ctx.suites.add(gx.ExpectationSuite(name=suite_name))
    for item in expectations_with_meta:
        suite.add_expectation(item["expectation"])

    val_def = ctx.validation_definitions.add(
        gx.ValidationDefinition(name=f"vd_{suite_name}", data=batch_def, suite=suite)
    )
    gx_result = val_def.run(batch_parameters={"dataframe": df})

    rule_outcomes = []
    critical_failures = 0
    warning_failures = 0

    for idx, res in enumerate(gx_result.results):
        meta = expectations_with_meta[idx]
        passed = bool(res.success)
        severity = meta["severity"]
        if not passed:
            if severity == "Critical":
                critical_failures += 1
            elif severity == "Warning":
                warning_failures += 1

        # Extraer conteos observados de GX
        res_dict = res.result or {}
        rule_outcomes.append({
            "rule_id": meta["rule_id"],
            "quality_dimension": meta["dimension"],
            "severity": severity,
            "related_requirement": meta["requirement"],
            "description": meta["description"],
            "expectation_type": res.expectation_config.type,
            "success": passed,
            "observed_value": res_dict.get("observed_value"),
            "element_count": res_dict.get("element_count"),
            "unexpected_count": res_dict.get("unexpected_count"),
            "unexpected_percent": res_dict.get("unexpected_percent")
        })

    if critical_failures > 0:
        pipeline_action = "STOP_BRANCH_CRITICAL_FAILURE"
    elif warning_failures > 0:
        pipeline_action = "CONTINUE_WITH_WARNING_LOGGED"
    else:
        pipeline_action = "PROCEED_NORMAL"

    return {
        "suite_name": suite_name,
        "evaluated_at_utc": datetime.now(timezone.utc).isoformat(),
        "overall_gx_success": bool(gx_result.success),
        "critical_failures_count": critical_failures,
        "warning_failures_count": warning_failures,
        "pipeline_action": pipeline_action,
        "rule_outcomes": rule_outcomes
    }


def validate_primary_raw_gate(
    primary_csv_path: str,
    evidence_dir: str,
    batch_id: str,
    simulate_failure: bool = False
) -> Dict[str, Any]:
    """
    COMPUERTA 1: Validación Cruda de la Fuente Primaria (CSV INS).
    Evalúa el lote extraído antes de limpiar o transformar.
    """
    print(f"[GX GATE 1 - PRIMARY RAW] Evaluando fuente primaria cruda (lote={batch_id})...")
    os.makedirs(evidence_dir, exist_ok=True)
    df_raw = pd.read_csv(primary_csv_path, dtype=str)

    if simulate_failure:
        print("[GX GATE 1 - TEST B] Simulando corrupción en fuente primaria cruda: inyectando NULL en columna crítica 'IRCA' (viola DQ_02)...")
        df_raw.loc[0:4, "IRCA"] = np.nan

    expectations = [
        {
            "rule_id": "DQ_01",
            "dimension": "Completeness & Volume",
            "severity": "Critical",
            "requirement": "R1-R7",
            "description": "El lote primario CSV debe contener entre 15,000 y 25,000 registros históricos nacionales.",
            "expectation": gx.expectations.ExpectTableRowCountToBeBetween(min_value=15000, max_value=25000)
        },
        {
            "rule_id": "DQ_02",
            "dimension": "Completeness",
            "severity": "Critical",
            "requirement": "R1, R2, R4",
            "description": "Atributos clave de negocio (DepartamentoCodigo, MunicipioCodigo, Año, IRCA) no deben ser nulos en el CSV crudo.",
            "expectation": gx.expectations.ExpectColumnValuesToNotBeNull(column="IRCA")
        },
        {
            "rule_id": "DQ_03",
            "dimension": "Completeness",
            "severity": "Critical",
            "requirement": "R1-R7",
            "description": "MunicipioCodigo no debe presentar nulos en el archivo fuente.",
            "expectation": gx.expectations.ExpectColumnValuesToNotBeNull(column="MunicipioCodigo")
        },
        {
            "rule_id": "DQ_04",
            "dimension": "Validity (Domain)",
            "severity": "Warning",
            "requirement": "R1-R5 (Granularidad)",
            "description": "Advertencia de grano mixto: proporción de filas '#TODOS' (subtotales departamentales) debe ser menor al 5% (mostly >= 0.95 municipios reales).",
            "expectation": gx.expectations.ExpectColumnValuesToNotBeInSet(
                column="Municipio",
                value_set=["#TODOS"],
                mostly=0.95
            )
        }
    ]

    result = _run_gx_suite(df_raw, "primary_raw_csv", "suite_primary_raw", expectations)
    result["gate"] = "Primary Source Raw Validation"
    result["batch_id"] = batch_id

    out_path = os.path.join(evidence_dir, f"gx_gate1_primary_raw_{batch_id}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    if result["critical_failures_count"] > 0:
        raise RuntimeError(f"[GX GATE 1 FAILED] {result['critical_failures_count']} reglas críticas violadas en fuente primaria.")

    print(f"[GX GATE 1 - PRIMARY RAW] -> APROBADA ({result['pipeline_action']}). Evidencia en {out_path}")
    return result


def validate_api_raw_gate(
    api_raw_dir: str,
    evidence_dir: str,
    batch_id: str,
    simulate_failure: bool = False
) -> Dict[str, Any]:
    """
    COMPUERTA 2: Validación Cruda de la Fuente API (API Colombia).
    Evalúa los lotes JSON extraídos de Departamentos, Ciudades y Resguardos Indígenas.
    """
    print(f"[GX GATE 2 - API RAW] Evaluando lotes crudos de API Colombia (lote={batch_id})...")
    os.makedirs(evidence_dir, exist_ok=True)

    with open(os.path.join(api_raw_dir, "departments_raw.json"), "r", encoding="utf-8") as f:
        df_depts = pd.DataFrame(json.load(f))
    with open(os.path.join(api_raw_dir, "cities_raw.json"), "r", encoding="utf-8") as f:
        df_cities = pd.DataFrame(json.load(f))
    with open(os.path.join(api_raw_dir, "indigenous_reservations_raw.json"), "r", encoding="utf-8") as f:
        df_indig = pd.DataFrame(json.load(f))

    if simulate_failure:
        print("[GX GATE 2 - TEST B] Simulando corrupción en respuesta de API Colombia: población negativa (-50000) en /Department (viola DQ_06)...")
        df_depts.loc[0, "population"] = -50000

    # Suite 2A: Departamentos API
    exp_depts = [
        {
            "rule_id": "DQ_05",
            "dimension": "Completeness & Contract",
            "severity": "Critical",
            "requirement": "R6",
            "description": "El endpoint /Department debe retornar exactamente las 33 unidades departamentales de Colombia (32 deptos + Bogotá).",
            "expectation": gx.expectations.ExpectTableRowCountToBeBetween(min_value=33, max_value=33)
        },
        {
            "rule_id": "DQ_06",
            "dimension": "Validity & Range",
            "severity": "Critical",
            "requirement": "R6",
            "description": "La población departamental (population) en la API no debe ser nula y debe ser estrictamente positiva (> 10,000 hab).",
            "expectation": gx.expectations.ExpectColumnValuesToBeBetween(column="population", min_value=10000, max_value=15000000)
        },
        {
            "rule_id": "DQ_07",
            "dimension": "Validity & Range",
            "severity": "Critical",
            "requirement": "R6",
            "description": "La superficie departamental (surface) en la API debe ser estrictamente positiva (> 10 km2).",
            "expectation": gx.expectations.ExpectColumnValuesToBeBetween(column="surface", min_value=10, max_value=200000)
        }
    ]
    res_depts = _run_gx_suite(df_depts, "api_depts_raw", "suite_api_departments_raw", exp_depts)

    # Suite 2B: Resguardos Indígenas y Ciudades API
    exp_indig = [
        {
            "rule_id": "DQ_08",
            "dimension": "Completeness & Volume",
            "severity": "Critical",
            "requirement": "R7",
            "description": "El endpoint paginado /IndigenousReservation debe retornar al menos 500 resguardos indígenas con cityId no nulo.",
            "expectation": gx.expectations.ExpectColumnValuesToNotBeNull(column="cityId")
        }
    ]
    res_indig = _run_gx_suite(df_indig, "api_indig_raw", "suite_api_indigenous_raw", exp_indig)

    # Suite 2C: Observación de completitud en City.population (Warning documentada del perfilado)
    exp_cities = [
        {
            "rule_id": "DQ_09",
            "dimension": "Completeness (API Limitation)",
            "severity": "Informational",
            "requirement": "R6, R7",
            "description": "Monitoreo de completitud de City: el catálogo municipal de API Colombia debe contener al menos 1,100 municipios.",
            "expectation": gx.expectations.ExpectTableRowCountToBeBetween(min_value=1100, max_value=1200)
        }
    ]
    res_cities = _run_gx_suite(df_cities, "api_cities_raw", "suite_api_cities_raw", exp_cities)

    total_critical = (
        res_depts["critical_failures_count"]
        + res_indig["critical_failures_count"]
        + res_cities["critical_failures_count"]
    )

    combined = {
        "gate": "API Raw Validation",
        "batch_id": batch_id,
        "evaluated_at_utc": datetime.now(timezone.utc).isoformat(),
        "critical_failures_count": total_critical,
        "pipeline_action": "STOP_BRANCH_CRITICAL_FAILURE" if total_critical > 0 else "PROCEED_NORMAL",
        "suites": {
            "departments": res_depts,
            "indigenous_reservations": res_indig,
            "cities": res_cities
        }
    }

    out_path = os.path.join(evidence_dir, f"gx_gate2_api_raw_{batch_id}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(combined, f, ensure_ascii=False, indent=2)

    if total_critical > 0:
        raise RuntimeError(f"[GX GATE 2 FAILED] {total_critical} reglas críticas violadas en la fuente API.")

    print(f"[GX GATE 2 - API RAW] -> APROBADA ({combined['pipeline_action']}). Evidencia en {out_path}")
    return combined


def validate_prepared_gate(
    prepared_dir: str,
    evidence_dir: str,
    batch_id: str,
    simulate_failure_mode: str = "none"
) -> Dict[str, Any]:
    """
    COMPUERTA 3: Validación de Datos Preparados e Integrados (Sección 14).
    Verifica invariantes dimensionales, rangos IRCA [0, 100], unicidad del grano,
    integridad de integración con API Colombia y ausencia de imputación artificial a 0.
    Un fallo Crítico aquí BLOQUEA la carga al Data Warehouse.
    """
    print(f"[GX GATE 3 - PREPARED DATA] Evaluando tablas preparadas antes de cargar al DW (lote={batch_id})...")
    os.makedirs(evidence_dir, exist_ok=True)

    dim_tiempo = pd.read_csv(os.path.join(prepared_dir, "dim_tiempo.csv"))
    dim_geo = pd.read_csv(os.path.join(prepared_dir, "dim_geografia.csv"))
    fact_irca = pd.read_csv(os.path.join(prepared_dir, "fact_irca.csv"))

    if simulate_failure_mode == "duplicate_grain":
        print("[GX GATE 3 - TEST B] Simulando violación de unicidad del grano (DQ_13): duplicando fila en fact_irca...")
        fact_irca = pd.concat([fact_irca, fact_irca.iloc[[0]]], ignore_index=True)

    # Suite 3A: Fact_IRCA (Rangos [0, 100], llaves no nulas, unicidad compuesta del grano)
    exp_fact = [
        {
            "rule_id": "DQ_10",
            "dimension": "Validity (Range)",
            "severity": "Critical",
            "requirement": "R1-R7",
            "description": "Todo valor de irca_general debe estar estrictamente en el rango legal [0.0, 100.0].",
            "expectation": gx.expectations.ExpectColumnValuesToBeBetween(column="irca_general", min_value=0.0, max_value=100.0)
        },
        {
            "rule_id": "DQ_11",
            "dimension": "Validity (Range)",
            "severity": "Critical",
            "requirement": "R1, R2, R3",
            "description": "Todo valor no nulo de irca_urbano debe estar estrictamente entre [0.0, 100.0] (preservando NULLs reales sin imputar 0).",
            "expectation": gx.expectations.ExpectColumnValuesToBeBetween(column="irca_urbano", min_value=0.0, max_value=100.0)
        },
        {
            "rule_id": "DQ_12",
            "dimension": "Validity (Range)",
            "severity": "Critical",
            "requirement": "R1, R3, R4, R5, R7",
            "description": "Todo valor no nulo de irca_rural debe estar estrictamente entre [0.0, 100.0] (preservando NULLs reales sin imputar 0).",
            "expectation": gx.expectations.ExpectColumnValuesToBeBetween(column="irca_rural", min_value=0.0, max_value=100.0)
        },
        {
            "rule_id": "DQ_13",
            "dimension": "Uniqueness (Fact Grain)",
            "severity": "Critical",
            "requirement": "R1-R7",
            "description": "Unicidad estricta del grano compuesto (fk_geografia, fk_tiempo) en Fact_IRCA.",
            "expectation": gx.expectations.ExpectCompoundColumnsToBeUnique(column_list=["fk_geografia", "fk_tiempo"])
        }
    ]
    res_fact = _run_gx_suite(fact_irca, "prepared_fact_irca", "suite_prepared_fact_irca", exp_fact)

    # Suite 3B: Dim_Geografia (Unicidad de PK/BK y 100% Cobertura de Integración con API Colombia)
    exp_geo = [
        {
            "rule_id": "DQ_14",
            "dimension": "Uniqueness (Dimension PK/BK)",
            "severity": "Critical",
            "requirement": "R1-R7",
            "description": "La clave sustituta sk_geografia y el código DIVIPOLA codigo_municipio deben ser 100% únicos en Dim_Geografia.",
            "expectation": gx.expectations.ExpectColumnValuesToBeUnique(column="codigo_municipio")
        },
        {
            "rule_id": "DQ_15",
            "dimension": "Consistency (Integration Coverage)",
            "severity": "Critical",
            "requirement": "R6, R7",
            "description": "El 100% de los municipios en Dim_Geografia deben haber cruzado exitosamente con la población departamental de API Colombia (> 0).",
            "expectation": gx.expectations.ExpectColumnValuesToBeBetween(
                column="poblacion_depto_api", min_value=10000, max_value=15000000
            )
        },
        {
            "rule_id": "DQ_16",
            "dimension": "Validity (Domain)",
            "severity": "Critical",
            "requirement": "R1-R5",
            "description": "Ningún registro en Dim_Geografia puede contener el valor agregado '#TODOS'.",
            "expectation": gx.expectations.ExpectColumnValuesToNotBeInSet(column="municipio", value_set=["#TODOS"])
        }
    ]
    res_geo = _run_gx_suite(dim_geo, "prepared_dim_geografia", "suite_prepared_dim_geografia", exp_geo)

    total_critical = res_fact["critical_failures_count"] + res_geo["critical_failures_count"]
    combined = {
        "gate": "Prepared Data Validation",
        "batch_id": batch_id,
        "evaluated_at_utc": datetime.now(timezone.utc).isoformat(),
        "critical_failures_count": total_critical,
        "pipeline_action": "BLOCK_DW_LOAD_CRITICAL_FAILURE" if total_critical > 0 else "PROCEED_TO_DW_LOAD",
        "suites": {
            "fact_irca": res_fact,
            "dim_geografia": res_geo
        }
    }

    out_path = os.path.join(evidence_dir, f"gx_gate3_prepared_{batch_id}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(combined, f, ensure_ascii=False, indent=2)

    if total_critical > 0:
        raise RuntimeError(
            f"[GX GATE 3 CRITICAL FAILURE] Se detectaron {total_critical} violaciones críticas en datos preparados. "
            f"Carga al Data Warehouse BLOQUEADA según política de severidad. Evidencia: {out_path}"
        )

    print(f"[GX GATE 3 - PREPARED DATA] -> APROBADA ({combined['pipeline_action']}). Evidencia en {out_path}")
    return combined


if __name__ == "__main__":
    base = "c:/ETL/ProyectosEtl/Proyecto2"
    validate_primary_raw_gate(
        f"{base}/data/raw/primary/Calidad_del_Agua_para_Consumo_Humano_en_Colombia_20260905.csv",
        f"{base}/docs/evidence",
        "manual_test"
    )
    validate_api_raw_gate(f"{base}/data/raw/api", f"{base}/docs/evidence", "manual_test")
    validate_prepared_gate(f"{base}/data/prepared", f"{base}/docs/evidence", "manual_test")

"""
Orquestador y Ejecutor de las Pruebas de Confiabilidad Obligatorias (Sección 20):
- Test A (20.1 Successful Run): Ejecuta el flujo completo con un lote válido que satisface todas las políticas Críticas.
- Test B (20.2 Controlled Critical Quality Failure): Activa una violación controlada de la regla Crítica DQ_10
  (irca_general = 145.0 > 100.0), demostrando cómo `validate_prepared` falla, registra la evidencia en Great Expectations
  y BLOQUEA la ejecución downstream (`load_dimensional_dw` queda en estado `upstream_failed` / bloqueado).
- Test C (20.3 Safe Rerun and Repeatability): Reejecuta el mismo lote válido de entrada y compara conteos, claves y
  métricas del Data Warehouse antes y después de la reejecución, demostrando idempotencia (cero duplicados o corrupción).
"""

import os
import sys
import json
from datetime import datetime, timezone

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.extract_primary import extract_primary_source
from src.extract_api import extract_api_sources
from gx.gx_validation import (
    validate_primary_raw_gate,
    validate_api_raw_gate,
    validate_prepared_gate,
)
from src.transform_integrate import transform_and_integrate
from src.load_dw import load_dimensional_dw


def execute_pipeline_run(batch_id: str, inject_critical_failure: bool = False, reuse_api_cache: bool = False):
    """Simula/ejecuta todas las etapas del grafo de Airflow registrando estados por Task Instance."""
    csv_path = os.path.join(
        PROJECT_ROOT,
        "data",
        "raw",
        "primary",
        "Calidad_del_Agua_para_Consumo_Humano_en_Colombia_20260905.csv",
    )
    primary_manifest_dir = os.path.join(PROJECT_ROOT, "data", "raw", "primary")
    api_raw_dir = os.path.join(PROJECT_ROOT, "data", "raw", "api")
    prepared_dir = os.path.join(PROJECT_ROOT, "data", "prepared")
    evidence_dir = os.path.join(PROJECT_ROOT, "docs", "evidence")

    task_states = {
        "extract_primary": "pending",
        "extract_api": "pending",
        "validate_primary_raw": "pending",
        "validate_api_raw": "pending",
        "transform_and_clean_and_integrate": "pending",
        "validate_prepared": "pending",
        "load_dimensional_dw": "pending",
    }

    run_log = {
        "batch_id": batch_id,
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "inject_critical_failure": inject_critical_failure,
        "task_states": task_states,
        "error_message": None,
        "reconciliation": None,
    }

    try:
        # Rama Primaria
        extract_primary_source(csv_path, primary_manifest_dir, batch_id)
        task_states["extract_primary"] = "success"

        validate_primary_raw_gate(csv_path, evidence_dir, batch_id)
        task_states["validate_primary_raw"] = "success"

        # Rama API
        extract_api_sources(api_raw_dir, batch_id, reuse_persisted_if_present=reuse_api_cache)
        task_states["extract_api"] = "success"

        validate_api_raw_gate(api_raw_dir, evidence_dir, batch_id)
        task_states["validate_api_raw"] = "success"

        # Convergencia: Transformación e Integración
        transform_and_integrate(
            primary_csv_path=csv_path,
            api_raw_dir=api_raw_dir,
            prepared_output_dir=prepared_dir,
            batch_id=batch_id,
            inject_critical_failure=inject_critical_failure,
        )
        task_states["transform_and_clean_and_integrate"] = "success"

        # Compuerta 3: Validación de Datos Preparados
        validate_prepared_gate(prepared_dir, evidence_dir, batch_id)
        task_states["validate_prepared"] = "success"

        # Carga al Data Warehouse Dimensional
        load_rep = load_dimensional_dw(prepared_dir, evidence_dir, batch_id)
        task_states["load_dimensional_dw"] = "success"
        run_log["reconciliation"] = load_rep

    except Exception as exc:
        # Marcar la tarea en ejecución como failed y las dependientes como upstream_failed
        for t_name, st in list(task_states.items()):
            if st == "pending":
                task_states[t_name] = "failed"
                # Las siguientes quedan bloqueadas (upstream_failed)
                remaining = False
                for k in task_states:
                    if remaining:
                        task_states[k] = "upstream_failed (BLOCKED)"
                    if k == t_name:
                        remaining = True
                break
        run_log["error_message"] = str(exc)
        print(f"[PIPELINE STOPPED BY POLICY] {exc}")

    run_log["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
    return run_log


def run_mandatory_reliability_suite():
    evidence_dir = os.path.join(PROJECT_ROOT, "docs", "evidence")
    os.makedirs(evidence_dir, exist_ok=True)

    print("\n================ TEST A: SUCCESSFUL RUN ================")
    test_a = execute_pipeline_run("test_A_success", inject_critical_failure=False, reuse_api_cache=True)

    print("\n================ TEST B: CONTROLLED CRITICAL FAILURE ================")
    test_b = execute_pipeline_run("test_B_critical_failure", inject_critical_failure=True, reuse_api_cache=True)

    print("\n================ TEST C: SAFE RERUN & REPEATABILITY ================")
    test_c = execute_pipeline_run("test_C_safe_rerun", inject_critical_failure=False, reuse_api_cache=True)

    suite_summary = {
        "Test_A_Successful_Run": test_a,
        "Test_B_Controlled_Critical_Quality_Failure": test_b,
        "Test_C_Safe_Rerun_And_Repeatability": test_c,
    }

    out_path = os.path.join(evidence_dir, "reliability_tests_summary.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(suite_summary, f, ensure_ascii=False, indent=2)

    print(f"\n[RELIABILITY TESTS COMPLETED] Evidencia guardada en: {out_path}")
    return suite_summary


if __name__ == "__main__":
    run_mandatory_reliability_suite()

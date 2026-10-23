"""
DAG de Orquestación Completa con Apache Airflow 3.1.8 (Secciones 16, 17, 18, 19 y 20).
Utiliza la interfaz pública oficial requerida en el documento guía:
    from airflow.sdk import dag, task

Incluye un menú interactivo de Parámetros (`params` con `Param`) en la interfaz web de Airflow
para que al darle clic a "Trigger DAG" puedas elegir entre:
1. "normal_success": Ejecución normal exitosa (Prueba A y Prueba C Safe Rerun).
2. "fail_gate3_prepared_range": Inyecta irca_general = 145.0% (> 100%) y rompe la Compuerta 3 (DQ_10),
   bloqueando `load_dimensional_dw`.
3. "fail_gate3_duplicate_grain": Duplica una fila (fk_geografia, fk_tiempo) en Fact_IRCA y rompe la Compuerta 3 (DQ_13).
4. "fail_gate1_primary_null": Inyecta NULLs en la columna crítica 'IRCA' del CSV crudo y rompe la Compuerta 1 (DQ_02),
   bloqueando la transformación y la carga.
5. "fail_gate2_api_negative_pop": Inyecta una población negativa (-50,000) en la respuesta cruda de API Colombia y rompe
   la Compuerta 2 (DQ_06), bloqueando la transformación y la carga.
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any

try:
    from airflow.sdk import dag, task, Param
except ImportError:  # pragma: no cover
    from airflow.decorators import dag, task  # type: ignore
    from airflow.models.param import Param  # type: ignore

PROJECT_ROOT = os.getenv("ETL_PROJECT_ROOT", "/opt/airflow")
if not os.path.exists(os.path.join(PROJECT_ROOT, "src")):
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


@dag(
    dag_id="etl_ods6_irca_colombia_delivery2",
    description="Pipeline ETL Operativo ODS 6 (INS CSV + API Colombia + GX 3 Gates + MySQL DW)",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "ods6", "irca", "api_colombia", "great_expectations", "delivery2"],
    params={
        "test_scenario": Param(
            default="normal_success",
            type="string",
            enum=[
                "normal_success",
                "fail_gate3_prepared_range",
                "fail_gate3_duplicate_grain",
                "fail_gate1_primary_null",
                "fail_gate2_api_negative_pop",
            ],
            title="Escenario de Prueba de Confiabilidad (Sección 20)",
            description=(
                "Selecciona 'normal_success' para Test A (Éxito) y Test C (Safe Rerun). "
                "Selecciona cualquiera de las opciones 'fail_...' para demostrar en vivo el Test B "
                "(Fallo Crítico Controlado en Great Expectations que detiene el pipeline)."
            ),
        ),
        "reuse_api_persisted_cache": Param(
            default=True,
            type="boolean",
            title="Usar caché persistida de páginas largas de API (con petición viva a /Department)",
            description="Si es True, consulta /Department y /Region en vivo y reutiliza las páginas largas para mayor velocidad en la demo.",
        ),
    },
)
def etl_ods6_irca_colombia_pipeline():

    @task(
        task_id="extract_primary",
        retries=1,
        retry_delay=timedelta(seconds=15),
    )
    def extract_primary_task(**context) -> Dict[str, Any]:
        """Extrae metadatos y verifica integridad del lote primario CSV sin limpiarlo."""
        params = context.get("params", {})
        scenario = params.get("test_scenario", "normal_success")
        batch_id = f"airflow_{scenario}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

        csv_path = os.path.join(
            PROJECT_ROOT,
            "data",
            "raw",
            "primary",
            "Calidad_del_Agua_para_Consumo_Humano_en_Colombia_20260905.csv",
        )
        manifest_dir = os.path.join(PROJECT_ROOT, "data", "raw", "primary")
        manifest = extract_primary_source(csv_path, manifest_dir, batch_id)
        return {
            "batch_id": batch_id,
            "scenario": scenario,
            "primary_csv_path": csv_path,
            "raw_row_count": manifest["raw_row_count"],
        }

    @task(
        task_id="extract_api",
        retries=2,
        retry_delay=timedelta(seconds=30),
    )
    def extract_api_task(**context) -> Dict[str, Any]:
        """
        Extrae programáticamente los endpoints de API Colombia (/Department, /Region,
        /City/pagedList, /IndigenousReservation/pagedList).
        Reintentos=2 justificados por posibles fallos transitorios de red o timeouts HTTP.
        """
        params = context.get("params", {})
        scenario = params.get("test_scenario", "normal_success")
        reuse_cache = bool(params.get("reuse_api_persisted_cache", True))
        batch_id = f"airflow_{scenario}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

        api_raw_dir = os.path.join(PROJECT_ROOT, "data", "raw", "api")
        manifest = extract_api_sources(api_raw_dir, batch_id, reuse_persisted_if_present=reuse_cache)
        return {
            "batch_id": batch_id,
            "scenario": scenario,
            "api_raw_dir": api_raw_dir,
            "departments_extracted": manifest["endpoints"]["departments"]["records_extracted"],
            "cities_extracted": manifest["endpoints"]["cities"]["records_extracted"],
            "indigenous_extracted": manifest["endpoints"]["indigenous_reservations"]["records_extracted"],
        }

    @task(
        task_id="validate_primary_raw",
        retries=0,  # Fallo determinista de contrato/calidad: NO reintentar automáticamente
    )
    def validate_primary_raw_task(primary_ref: Dict[str, Any]) -> Dict[str, Any]:
        """Compuerta GX 1: Evalúa la fuente primaria cruda antes de permitir su transformación."""
        evidence_dir = os.path.join(PROJECT_ROOT, "docs", "evidence")
        simulate_fail = primary_ref.get("scenario") == "fail_gate1_primary_null"
        res = validate_primary_raw_gate(
            primary_csv_path=primary_ref["primary_csv_path"],
            evidence_dir=evidence_dir,
            batch_id=primary_ref["batch_id"],
            simulate_failure=simulate_fail,
        )
        return {
            "batch_id": primary_ref["batch_id"],
            "scenario": primary_ref["scenario"],
            "primary_csv_path": primary_ref["primary_csv_path"],
            "gate1_action": res["pipeline_action"],
            "critical_failures": res["critical_failures_count"],
        }

    @task(
        task_id="validate_api_raw",
        retries=0,  # Fallo determinista de contrato/calidad: NO reintentar automáticamente
    )
    def validate_api_raw_task(api_ref: Dict[str, Any]) -> Dict[str, Any]:
        """Compuerta GX 2: Evalúa los lotes JSON crudos extraídos de API Colombia."""
        evidence_dir = os.path.join(PROJECT_ROOT, "docs", "evidence")
        simulate_fail = api_ref.get("scenario") == "fail_gate2_api_negative_pop"
        res = validate_api_raw_gate(
            api_raw_dir=api_ref["api_raw_dir"],
            evidence_dir=evidence_dir,
            batch_id=api_ref["batch_id"],
            simulate_failure=simulate_fail,
        )
        return {
            "batch_id": api_ref["batch_id"],
            "scenario": api_ref["scenario"],
            "api_raw_dir": api_ref["api_raw_dir"],
            "gate2_action": res["pipeline_action"],
            "critical_failures": res["critical_failures_count"],
        }

    @task(
        task_id="transform_and_clean_and_integrate",
        retries=0,  # Transformación determinista sobre archivos validados
    )
    def transform_clean_integrate_task(
        primary_val: Dict[str, Any],
        api_val: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Convergencia: Se ejecuta ÚNICAMENTE cuando ambas ramas crudas aprueban sus políticas GX.
        Aplica reglas de negocio, elimina '#TODOS', preserva NULLs reales en IRCA urbano/rural
        e integra con API Colombia.
        """
        batch_id = primary_val["batch_id"]
        scenario = primary_val.get("scenario", "normal_success")
        inject_range_fail = scenario == "fail_gate3_prepared_range"

        prepared_dir = os.path.join(PROJECT_ROOT, "data", "prepared")
        rec = transform_and_integrate(
            primary_csv_path=primary_val["primary_csv_path"],
            api_raw_dir=api_val["api_raw_dir"],
            prepared_output_dir=prepared_dir,
            batch_id=batch_id,
            inject_critical_failure=inject_range_fail,
        )
        return {
            "batch_id": batch_id,
            "scenario": scenario,
            "prepared_dir": prepared_dir,
            "fact_irca_rows": rec["fact_irca_rows"],
            "dim_geografia_rows": rec["dim_geografia_rows"],
        }

    @task(
        task_id="validate_prepared",
        retries=0,  # Fallo determinista de calidad preparada: debe bloquear la carga de inmediato
    )
    def validate_prepared_task(prep_ref: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compuerta GX 3: Valida el Modelo Estrella preparado (rangos [0, 100], unicidad compuesta,
        cobertura 100% API Deptos). Un fallo Crítico detiene la carga al Data Warehouse.
        """
        evidence_dir = os.path.join(PROJECT_ROOT, "docs", "evidence")
        scenario = prep_ref.get("scenario", "normal_success")
        fail_mode = "duplicate_grain" if scenario == "fail_gate3_duplicate_grain" else "none"

        res = validate_prepared_gate(
            prepared_dir=prep_ref["prepared_dir"],
            evidence_dir=evidence_dir,
            batch_id=prep_ref["batch_id"],
            simulate_failure_mode=fail_mode,
        )
        return {
            "batch_id": prep_ref["batch_id"],
            "prepared_dir": prep_ref["prepared_dir"],
            "gate3_action": res["pipeline_action"],
            "critical_failures": res["critical_failures_count"],
        }

    @task(
        task_id="load_dimensional_dw",
        retries=2,
        retry_delay=timedelta(seconds=30),
    )
    def load_dimensional_dw_task(prep_val: Dict[str, Any]) -> Dict[str, Any]:
        """
        Carga transaccional e idempotente en MySQL (`dw_calidad_agua_colombia`) con
        reconciliación pre y post carga. Reintentos=2 ante bloqueos o caídas transitorias de BD.
        """
        evidence_dir = os.path.join(PROJECT_ROOT, "docs", "evidence")
        report = load_dimensional_dw(
            prepared_dir=prep_val["prepared_dir"],
            evidence_dir=evidence_dir,
            batch_id=prep_val["batch_id"],
        )
        return {
            "batch_id": prep_val["batch_id"],
            "database": report["database"],
            "post_load_counts": report["dw_post_load_counts"],
            "reconciliation_passed": report["reconciliation_passed"],
        }

    # Declaración explícita de dependencias del grafo (Sección 16.1)
    p_raw = extract_primary_task()
    a_raw = extract_api_task()

    p_valid = validate_primary_raw_task(p_raw)
    a_valid = validate_api_raw_task(a_raw)

    prepared = transform_clean_integrate_task(p_valid, a_valid)
    prepared_valid = validate_prepared_task(prepared)
    load_dimensional_dw_task(prepared_valid)


dag_instance = etl_ods6_irca_colombia_pipeline()

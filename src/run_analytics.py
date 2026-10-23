"""
Script que ejecuta todas las consultas analíticas R1-R7 contra el Data Warehouse en MySQL
y guarda los resultados exactos en docs/evidence/analytical_results_r1_r7.json para
respaldar con cifras concretas los hallazgos del README y del Dashboard.
"""

import os
import sys
import json
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.load_dw import get_db_uris


def run_all_analytical_queries():
    uris = get_db_uris()
    eng = create_engine(uris["db_uri"])

    results = {}
    with eng.connect() as conn:
        # R1: Serie histórica nacional (2007 y 2024 + resumen)
        r1_rows = [
            dict(r) for r in conn.execute(text("""
                SELECT t.anio,
                       COUNT(f.fk_geografia) AS municipios_evaluados,
                       ROUND(AVG(f.irca_general), 2) AS prom_irca_general,
                       ROUND(AVG(f.irca_urbano), 2) AS prom_irca_urbano,
                       ROUND(AVG(f.irca_rural), 2) AS prom_irca_rural
                FROM fact_irca f
                JOIN dim_tiempo t ON f.fk_tiempo = t.sk_tiempo
                GROUP BY t.anio
                ORDER BY t.anio ASC
            """)).mappings()
        ]
        results["R1_historical_trend"] = r1_rows

        # R2 Comparación: Ranking sesgado antiguo (COALESCE a 0) vs Ranking corregido (Entrega 2)
        r2_old_biased = [
            dict(r) for r in conn.execute(text("""
                SELECT g.departamento,
                       ROUND(AVG((COALESCE(f.irca_urbano, 0) + COALESCE(f.irca_rural, 0)) / 2), 2) AS prom_riesgo_sesgado_entrega1
                FROM fact_irca f
                JOIN dim_geografia g ON f.fk_geografia = g.sk_geografia
                JOIN dim_tiempo t ON f.fk_tiempo = t.sk_tiempo
                WHERE t.anio = (SELECT MAX(anio) FROM dim_tiempo)
                GROUP BY g.departamento
                ORDER BY prom_riesgo_sesgado_entrega1 DESC
                LIMIT 5
            """)).mappings()
        ]
        r2_corrected = [
            dict(r) for r in conn.execute(text("""
                SELECT g.departamento,
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
                LIMIT 5
            """)).mappings()
        ]
        results["R2_comparison"] = {
            "delivery1_biased_coalesce_zero": r2_old_biased,
            "delivery2_corrected_true_irca": r2_corrected
        }

        # R3: Top 5 Departamentos con mayor brecha rural-urbana
        r3_rows = [
            dict(r) for r in conn.execute(text("""
                SELECT g.departamento,
                       ROUND(AVG(f.irca_urbano), 2) AS prom_urbano,
                       ROUND(AVG(f.irca_rural), 2) AS prom_rural,
                       ROUND(AVG(f.irca_rural) - AVG(f.irca_urbano), 2) AS brecha_departamental,
                       ROUND(AVG(f.brecha_rural_urbana), 2) AS brecha_municipal_pareada
                FROM fact_irca f
                JOIN dim_geografia g ON f.fk_geografia = g.sk_geografia
                GROUP BY g.departamento
                HAVING prom_rural IS NOT NULL AND prom_urbano IS NOT NULL
                ORDER BY brecha_departamental DESC
                LIMIT 8
            """)).mappings()
        ]
        results["R3_rural_urban_gap_top"] = r3_rows

        # R4: Municipios en Alto Riesgo constante durante los últimos 5 años (2020-2024)
        r4_rows = [
            dict(r) for r in conn.execute(text("""
                SELECT g.codigo_municipio,
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
                ORDER BY prom_irca_general_5a DESC
            """)).mappings()
        ]
        results["R4_chronic_high_risk_municipalities_count"] = len(r4_rows)
        results["R4_chronic_high_risk_top10"] = r4_rows[:10]

        # R5: Mayor reducción histórica de riesgo rural (2007 vs 2024)
        r5_rows = [
            dict(r) for r in conn.execute(text("""
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
                SELECT a.departamento,
                       ROUND(a.irca_base, 2) AS riesgo_rural_base_2007,
                       ROUND(b.irca_actual, 2) AS riesgo_rural_actual_2024,
                       ROUND(b.irca_actual - a.irca_base, 2) AS reduccion_puntos_irca
                FROM min_anio a
                JOIN max_anio b ON a.departamento = b.departamento
                WHERE a.irca_base IS NOT NULL AND b.irca_actual IS NOT NULL
                  AND (b.irca_actual - a.irca_base) < 0
                ORDER BY reduccion_puntos_irca ASC
                LIMIT 8
            """)).mappings()
        ]
        results["R5_rural_improvement_leaders"] = r5_rows

        # R6: Exposición demográfica departamental al riesgo hídrico (Integración CSV + API /Department)
        r6_rows = [
            dict(r) for r in conn.execute(text("""
                SELECT g.region_geografica,
                       g.departamento,
                       MAX(g.poblacion_depto_api) AS poblacion_departamental_api,
                       MAX(g.densidad_depto_hab_km2) AS densidad_hab_km2_api,
                       COUNT(f.fk_geografia) AS total_municipios_evaluados,
                       SUM(CASE WHEN f.irca_general > 14.0 THEN 1 ELSE 0 END) AS municipios_riesgo_medio_o_alto,
                       ROUND(SUM(CASE WHEN f.irca_general > 14.0 THEN 1 ELSE 0 END) * 100.0 / COUNT(f.fk_geografia), 2) AS pct_municipios_afectados,
                       ROUND(AVG(f.irca_general), 2) AS prom_irca_depto,
                       ROUND(MAX(g.poblacion_depto_api) * (SUM(CASE WHEN f.irca_general > 14.0 THEN 1 ELSE 0 END) / COUNT(f.fk_geografia)), 0) AS poblacion_estimada_expuesta_riesgo
                FROM fact_irca f
                JOIN dim_geografia g ON f.fk_geografia = g.sk_geografia
                JOIN dim_tiempo t ON f.fk_tiempo = t.sk_tiempo
                WHERE t.anio = (SELECT MAX(anio) FROM dim_tiempo)
                GROUP BY g.region_geografica, g.departamento
                ORDER BY poblacion_estimada_expuesta_riesgo DESC
            """)).mappings()
        ]
        results["R6_demographic_exposure_top10"] = r6_rows[:10]
        results["R6_total_estimated_exposed_population_2024"] = int(sum(float(x["poblacion_estimada_expuesta_riesgo"] or 0) for x in r6_rows))

        # R7: Brecha en municipios con Resguardos Indígenas (Integración CSV + API /IndigenousReservation)
        r7_summary = [
            dict(r) for r in conn.execute(text("""
                SELECT CASE WHEN g.tiene_resguardo_indigena = 1 THEN 'Con Resguardo Indigena (API)' ELSE 'Sin Resguardo Indigena' END AS condicion_territorial,
                       COUNT(DISTINCT g.sk_geografia) AS cantidad_municipios,
                       ROUND(AVG(f.irca_general), 2) AS prom_irca_general_historico,
                       ROUND(AVG(f.irca_rural), 2) AS prom_irca_rural_historico,
                       ROUND(AVG(CASE WHEN t.anio = 2024 THEN f.irca_general END), 2) AS prom_irca_general_2024,
                       ROUND(AVG(CASE WHEN t.anio = 2024 THEN f.irca_rural END), 2) AS prom_irca_rural_2024
                FROM fact_irca f
                JOIN dim_geografia g ON f.fk_geografia = g.sk_geografia
                JOIN dim_tiempo t ON f.fk_tiempo = t.sk_tiempo
                GROUP BY g.tiene_resguardo_indigena
                ORDER BY g.tiene_resguardo_indigena DESC
            """)).mappings()
        ]
        r7_top_critical = [
            dict(r) for r in conn.execute(text("""
                SELECT g.codigo_municipio,
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
                WHERE t.anio = 2024 AND g.tiene_resguardo_indigena = 1
                ORDER BY f.irca_general DESC
                LIMIT 10
            """)).mappings()
        ]
        results["R7_indigenous_territories_analysis"] = {
            "group_comparison": r7_summary,
            "top10_critical_indigenous_municipalities_2024": r7_top_critical
        }

    out_path = "c:/ETL/ProyectosEtl/Proyecto2/docs/evidence/analytical_results_r1_r7.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, default=str, ensure_ascii=False, indent=2)

    print(f"[ANALYTICS] Resultados R1-R7 generados en: {out_path}")
    return results


if __name__ == "__main__":
    import sys
    sys.path.insert(0, "c:/ETL/ProyectosEtl/Proyecto2")
    res = run_all_analytical_queries()
    print("R2 Comparison:", json.dumps(res["R2_comparison"], default=str, ensure_ascii=False, indent=2))
    print("R6 Top 3:", json.dumps(res["R6_demographic_exposure_top10"][:3], default=str, ensure_ascii=False, indent=2))
    print("R7 Summary:", json.dumps(res["R7_indigenous_territories_analysis"]["group_comparison"], default=str, ensure_ascii=False, indent=2))

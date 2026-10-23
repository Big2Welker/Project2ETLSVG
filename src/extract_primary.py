"""
Módulo de Extracción de la Fuente Primaria (CSV INS - Calidad del Agua IRCA).
Preserva evidencia del lote crudo (hash MD5, conteo de filas, columnas y timestamp)
sin aplicar limpiezas silenciosas antes de la compuerta de validación cruda.
"""

import os
import json
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any
import pandas as pd


def _compute_md5(file_path: str) -> str:
    hasher = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def extract_primary_source(file_path: str, output_manifest_dir: str, batch_id: str) -> Dict[str, Any]:
    """
    Verifica la existencia del archivo primario CSV, lee su estructura cruda
    sin modificar ningún valor y registra metadatos de trazabilidad del lote.
    """
    print(f"[EXTRACT PRIMARY] Iniciando lectura cruda desde: {file_path} (lote={batch_id})")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"[ERROR] El archivo fuente primario no existe: {file_path}")

    df_raw = pd.read_csv(file_path, dtype=str)
    os.makedirs(output_manifest_dir, exist_ok=True)

    extraction_ts = datetime.now(timezone.utc).isoformat()
    file_md5 = _compute_md5(file_path)

    manifest = {
        "batch_id": batch_id,
        "extraction_timestamp_utc": extraction_ts,
        "source_file": file_path,
        "file_md5": file_md5,
        "raw_row_count": int(len(df_raw)),
        "raw_columns": list(df_raw.columns)
    }

    manifest_path = os.path.join(output_manifest_dir, f"primary_manifest_{batch_id}.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print(f"[EXTRACT PRIMARY] -> {len(df_raw)} registros crudos detectados (MD5={file_md5[:10]}...).")
    return manifest

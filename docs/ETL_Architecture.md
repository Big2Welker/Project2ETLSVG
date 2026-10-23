# Arquitectura de Alto Nivel del Sistema (Segunda Entrega)

Este diagrama representa las fuentes de datos, capas de persistencia cruda y preparada, las **3 compuertas de validación con Great Expectations**, el flujo de transformación e integración, la carga idempotente al Data Warehouse en MySQL, la capa analítica de BI y la orquestación completa con **Apache Airflow 3.1.8**.

```mermaid
flowchart TB
    classDef source fill:#E2EFDA,stroke:#548235,stroke-width:2px,color:black
    classDef raw fill:#EDEDED,stroke:#7F7F7F,stroke-width:2px,color:black
    classDef gate fill:#FFF2CC,stroke:#D6B656,stroke-width:2px,color:black
    classDef transform fill:#DDEBF7,stroke:#2E75B6,stroke-width:2px,color:black
    classDef dw fill:#FCE4D6,stroke:#C55A11,stroke-width:2px,color:black
    classDef bi fill:#E8D8F8,stroke:#7030A0,stroke-width:2px,color:black

    subgraph FUENTES["1. Fuentes de Datos"]
        CSV[("Fuente Primaria (CSV INS)\nCalidad del Agua IRCA 2007-2024\n19,160 registros")]:::source
        API[("Fuente Secundaria (API Colombia)\n/Department, /Region,\n/City/pagedList, /IndigenousReservation")]:::source
    end

    subgraph EXTRACCION["2. Adquisición y Persistencia Cruda (Airflow Tasks)"]
        EXT_P["extract_primary\n(Hash MD5, Manifiesto JSON)\nretries=1"]:::raw
        EXT_A["extract_api\n(Paginación REST, Backoff)\nretries=2"]:::raw
    end

    subgraph COMPUERTAS_RAW["3. Compuertas de Validación Cruda (Great Expectations)"]
        GATE1{"validate_primary_raw\n(Suite: DQ_01 a DQ_04)\nretries=0"}:::gate
        GATE2{"validate_api_raw\n(Suite: DQ_05 a DQ_09)\nretries=0"}:::gate
    end

    subgraph TRANSFORMACION["4. Transformación, Limpieza y Contrato de Integración"]
        TRANS["transform_and_clean_and_integrate\n• Exclusión grano mixto (#TODOS: 569 filas)\n• 'ND' -> NULL real (NUNCA 0)\n• Homologación DIVIPOLA y Deptos (33/33)\n• Enriquecimiento Población y Resguardos"]:::transform
        PREP[("Capa Preparada\ndim_tiempo (18)\ndim_geografia (1,095)\nfact_irca (18,591)")]:::raw
    end

    subgraph COMPUERTA_PREP["5. Compuerta de Datos Preparados (Great Expectations)"]
        GATE3{"validate_prepared\n(Suite: DQ_10 a DQ_16)\nRangos [0,100], Unicidad Grano,\nCobertura 100% API\nretries=0"}:::gate
        STOP["BLOQUEO CRÍTICO\n(upstream_failed)\nProtege el DW"]:::gate
    end

    subgraph CARGA_DW["6. Data Warehouse Dimensional y Analítica"]
        LOAD["load_dimensional_dw\n(Transaccional Idempotente +\nReconciliación Pre/Post)\nretries=2"]:::dw
        MYSQL[("MySQL Data Warehouse\ndw_calidad_agua_colombia\nPK/FK/Unique Enforced")]:::dw
        BI["Consultas SQL (R1-R7)\ny Dashboard Power BI"]:::bi
    end

    CSV --> EXT_P --> GATE1
    API --> EXT_A --> GATE2
    GATE1 -->|Aprobada| TRANS
    GATE2 -->|Aprobada| TRANS
    TRANS --> PREP --> GATE3
    GATE3 -->|Violación Crítica| STOP
    GATE3 -->|Aprobada| LOAD --> MYSQL --> BI
```

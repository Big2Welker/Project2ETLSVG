# Modelo Dimensional Refinado (Esquema Estrella - Segunda Entrega)

Este diagrama muestra la evolución desde el modelo base de la Primera Entrega hacia el **Esquema Estrella Refinado** de la Segunda Entrega, incorporando las claves oficiales DANE DIVIPOLA, el atributo consolidado `irca_general` (que corrige el sesgo de `COALESCE` en R2) y el enriquecimiento demográfico e indígena proveniente de **API Colombia** para responder R1–R7.

```mermaid
erDiagram
    dim_geografia ||--o{ fact_irca : "Filtra territorialmente (1:N)"
    dim_tiempo ||--o{ fact_irca : "Filtra temporalmente (1:N)"

    dim_tiempo {
        int sk_tiempo PK "Clave sustituta temporal"
        int anio UK "Año calendario (2007-2024)"
        int decada "Década (2000, 2010, 2020)"
        tinyint es_lustro_reciente "1 si año entre 2020 y 2024 (Soporta R4)"
    }

    dim_geografia {
        int sk_geografia PK "Clave sustituta geográfica"
        varchar codigo_departamento "Código DANE Depto (2 dígitos)"
        varchar departamento "Nombre oficial Departamento"
        varchar codigo_municipio UK "Código DANE DIVIPOLA Municipio (Business Key)"
        varchar municipio "Nombre oficial Municipio"
        varchar region_geografica "Región natural vía API Colombia (/Region)"
        bigint poblacion_depto_api "Población departamental vía API Colombia (Soporta R6)"
        decimal superficie_depto_km2_api "Superficie km2 vía API Colombia (Soporta R6)"
        decimal densidad_depto_hab_km2 "Densidad hab/km2 derivada (Soporta R6)"
        int total_resguardos_indigenas "Conteo resguardos vía API Colombia (Soporta R7)"
        tinyint tiene_resguardo_indigena "1=Con resguardo, 0=Sin resguardo (Soporta R7)"
    }

    fact_irca {
        int fk_geografia PK, FK "Referencia a dim_geografia(sk_geografia)"
        int fk_tiempo PK, FK "Referencia a dim_tiempo(sk_tiempo)"
        decimal irca_general "IRCA consolidado municipal INS [0-100] (100% completitud, corrige R2)"
        decimal irca_urbano "IRCA urbano [0-100] (NULL real si 'ND', nunca se imputa 0)"
        decimal irca_rural "IRCA rural [0-100] (NULL real si 'ND', nunca se imputa 0)"
        decimal brecha_rural_urbana "irca_rural - irca_urbano cuando ambos existen (Soporta R3)"
        varchar nivel_riesgo_general "Clasificación normativa Res. 2115/2007"
    }
```

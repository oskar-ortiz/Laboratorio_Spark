# Laboratorio Spark

Entorno local y ETL del Online Retail Dataset (UCI) con PySpark 3.5.6.

## Estado

- Java 17 portable: `.tools/jdk-17/`
- PySpark 3.5.6 en `.venv/`
- Hadoop Windows: `.hadoop/bin/winutils.exe` + `hadoop.dll`
- Dataset: `data/raw/Online Retail.xlsx` y `data/raw/online_retail.csv` (541909 filas)
- Resultados del taller: `data/output/` + `CONCLUSIONES.md`

## Activar el entorno

PowerShell:

```powershell
. .\scripts\activate.ps1
```

CMD:

```bat
call scripts\activate.bat
```

## Regenerar el CSV desde el Excel

```powershell
python .\scripts\prepare_dataset.py
```

## Prueba minima de PySpark

```powershell
python .\scripts\smoke_test.py
```

## Ejecutar el ETL del taller

```powershell
python .\scripts\etl_online_retail.py
```

El script responde las 10 preguntas del taller, escribe un CSV por pregunta en `data/output/` y regenera `CONCLUSIONES.md`.

### Archivos de salida

| Archivo | Pregunta |
| --- | --- |
| `q01_total_facturas.csv` | Total de facturas |
| `q02_clientes_unicos.csv` | Clientes unicos |
| `q03_ingreso_total.csv` | Ingreso total neto |
| `q04_producto_mas_vendido.csv` | Producto mas vendido |
| `q05_cliente_mayor_compra.csv` | Cliente con mayor compra |
| `q06_top_5_paises.csv` | Top 5 paises fuera de UK |
| `q07_ticket_promedio.csv` | Ticket promedio |
| `q08_productos_por_factura.csv` | Min/max/avg productos por factura |
| `q09_mes_mas_ventas.csv` | Mes con mas ventas |
| `q10_porcentaje_devoluciones.csv` | % facturas con devoluciones |
| `ranking_clientes_top_100.csv` | Ranking opcional con `rank()` |

## Criterios usados

- Ingreso neto: `Quantity * UnitPrice` (incluye cantidades negativas).
- Devoluciones: `Quantity < 0` o `InvoiceNo` que empieza por `C`.
- Producto mas vendido: solo cantidades positivas.
- Productos por factura: referencias `StockCode` distintas.

## Architecture & Data Processing Topology

```mermaid
flowchart TD
    Source[Online Retail Dataset
Excel + CSV raw files] --> Ingest[Data Preparation Layer
prepare_dataset.py]
    Ingest --> Raw[(data/raw/)
Raw source files]
    Raw --> Spark[PySpark Session
Spark engine + local cluster config]

    Spark --> ETL[ETL Processing Layer
etl_online_retail.py]
    ETL --> Clean[Transformation & Quality Rules
normalization, filtering, aggregations]
    Clean --> Analysis[Business Queries
10 analytical questions]
    Analysis --> Output[(data/output/)
CSV reports]
    Output --> Summary[CONCLUSIONES.md
Executive findings]

    subgraph LocalStack[Local execution environment]
        Spark
        ETL
        Clean
        Analysis
    end
```

### Topology Notes
- El punto de entrada del flujo es la preparación del dataset local, donde el Excel se convierte a CSV para asegurar un procesamiento reproducible con PySpark.
- La capa de Spark centraliza la ejecución del ETL y la computación distribuida lógica en un entorno local, validado para datos transaccionales de retail.
- La etapa de transformación aplica reglas de negocio y calidad para calcular métricas de negocio, ingresos, devoluciones y patrones de comportamiento.
- La salida final se materializa en archivos CSV por consulta y también en `CONCLUSIONES.md`, facilitando tanto la trazabilidad técnica como la presentación de resultados.
- La arquitectura mantiene una separación clara entre ingestión, transformación, análisis y entrega, lo que facilita la escalabilidad y la auditoría del proceso.

### Architecture Summary
- Source ingestion: `Excel / CSV -> raw data`
- Processing engine: `PySpark 3.5.6`
- Workflow: `prepare_dataset -> ETL -> transformations -> outputs`
- Final artifacts: `data/output/*.csv` + `CONCLUSIONES.md`
- Design principle: deterministic local ETL with analytical reporting by layer

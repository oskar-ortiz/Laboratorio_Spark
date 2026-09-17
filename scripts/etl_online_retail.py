from pathlib import Path
import csv
import shutil

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType
from pyspark.sql.window import Window


PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_PATH = PROJECT_ROOT / "data" / "raw" / "online_retail.csv"
OUTPUT_DIR = PROJECT_ROOT / "data" / "output"
CONCLUSIONS_PATH = PROJECT_ROOT / "CONCLUSIONES.md"

SCHEMA = StructType(
    [
        StructField("InvoiceNo", StringType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("InvoiceDate", StringType(), True),
        StructField("UnitPrice", DoubleType(), True),
        StructField("CustomerID", DoubleType(), True),
        StructField("Country", StringType(), True),
    ]
)


def write_csv(dataframe: DataFrame, filename: str) -> None:
    """Escribe un CSV unico sin depender de Hadoop NativeIO ni pandas."""
    target = OUTPUT_DIR / filename
    temporary = OUTPUT_DIR / f"_{filename}.tmp"
    if temporary.exists():
        if temporary.is_dir():
            shutil.rmtree(temporary)
        else:
            temporary.unlink()
    if target.exists():
        target.unlink()

    rows = dataframe.collect()
    columns = dataframe.columns
    with target.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row[column] for column in columns})


def scalar(dataframe: DataFrame, column: str):
    return dataframe.first()[column]


def format_number(value, decimals: int = 2) -> str:
    return f"{float(value):,.{decimals}f}"


def main() -> None:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"No se encontro el dataset: {INPUT_PATH}")

    spark = (
        SparkSession.builder
        .appName("OnlineRetailETL")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", str(PROJECT_ROOT / "data" / "spark-warehouse"))
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        raw = (
            spark.read
            .option("header", True)
            .option("encoding", "UTF-8")
            .schema(SCHEMA)
            .csv(str(INPUT_PATH))
        )
        selected = (
            raw.select(
                "InvoiceNo",
                "StockCode",
                "Description",
                "Quantity",
                "InvoiceDate",
                "UnitPrice",
                "CustomerID",
                "Country",
            )
            .withColumn(
                "InvoiceDate",
                F.to_timestamp("InvoiceDate", "yyyy-MM-dd HH:mm:ss"),
            )
        )
        cleaned = (
            selected
            .withColumn("line_total", F.col("Quantity") * F.col("UnitPrice"))
            .withColumn("invoice_month", F.month("InvoiceDate"))
            .withColumn(
                "is_return",
                (F.col("Quantity") < 0) | F.col("InvoiceNo").startswith("C"),
            )
            .filter(F.col("InvoiceNo").isNotNull())
        )
        invoice_totals = cleaned.groupBy("InvoiceNo").agg(
            F.sum("line_total").alias("invoice_total"),
            F.sum("Quantity").alias("invoice_quantity"),
            F.countDistinct("StockCode").alias("product_references"),
            F.max(F.col("is_return").cast("int")).alias("has_return"),
        )

        total_invoices = invoice_totals.agg(F.count("InvoiceNo").alias("value"))
        unique_customers = cleaned.filter(F.col("CustomerID").isNotNull()).agg(
            F.countDistinct("CustomerID").alias("value")
        )
        total_revenue = cleaned.agg(F.sum("line_total").alias("value"))

        product_sales = (
            cleaned.filter((F.col("Quantity") > 0) & F.col("StockCode").isNotNull())
            .groupBy("StockCode", "Description")
            .agg(F.sum("Quantity").alias("quantity_sold"))
            .orderBy(F.desc("quantity_sold"), F.asc("StockCode"))
        )
        top_product = product_sales.limit(1)

        customer_totals = (
            cleaned.filter(F.col("CustomerID").isNotNull())
            .groupBy("CustomerID")
            .agg(F.sum("line_total").alias("customer_total"))
            .orderBy(F.desc("customer_total"), F.asc("CustomerID"))
        )
        top_customer = customer_totals.limit(1)

        country_totals = (
            cleaned.filter(F.col("Country") != "United Kingdom")
            .groupBy("Country")
            .agg(
                F.sum("line_total").alias("country_total"),
                F.sum("Quantity").alias("quantity_sold"),
            )
            .orderBy(F.desc("country_total"), F.asc("Country"))
        )
        top_countries = country_totals.limit(5)

        ticket_average = invoice_totals.agg(F.avg("invoice_total").alias("value"))
        products_per_invoice = invoice_totals.agg(
            F.min("product_references").alias("minimum"),
            F.max("product_references").alias("maximum"),
            F.avg("product_references").alias("average"),
        )
        monthly_sales = (
            cleaned.groupBy("invoice_month")
            .agg(F.sum("line_total").alias("monthly_total"))
            .orderBy(F.desc("monthly_total"), F.asc("invoice_month"))
        )
        top_month = monthly_sales.limit(1)
        return_invoices = invoice_totals.filter(F.col("has_return") == 1).agg(
            F.count("InvoiceNo").alias("return_invoices")
        )
        return_percentage = return_invoices.crossJoin(total_invoices).select(
            (F.col("return_invoices") / F.col("value") * 100).alias("percentage")
        )

        # Ranking de clientes solicitado como operacion opcional de ventana.
        customer_ranking = customer_totals.withColumn(
            "customer_rank",
            F.rank().over(Window.orderBy(F.desc("customer_total"))),
        )

        write_csv(total_invoices, "q01_total_facturas.csv")
        write_csv(unique_customers, "q02_clientes_unicos.csv")
        write_csv(total_revenue, "q03_ingreso_total.csv")
        write_csv(top_product, "q04_producto_mas_vendido.csv")
        write_csv(top_customer, "q05_cliente_mayor_compra.csv")
        write_csv(top_countries, "q06_top_5_paises.csv")
        write_csv(ticket_average, "q07_ticket_promedio.csv")
        write_csv(products_per_invoice, "q08_productos_por_factura.csv")
        write_csv(top_month, "q09_mes_mas_ventas.csv")
        write_csv(return_percentage, "q10_porcentaje_devoluciones.csv")
        write_csv(customer_ranking.limit(100), "ranking_clientes_top_100.csv")

        total_invoices_value = scalar(total_invoices, "value")
        unique_customers_value = scalar(unique_customers, "value")
        total_revenue_value = scalar(total_revenue, "value")
        product_row = top_product.first()
        customer_row = top_customer.first()
        country_rows = top_countries.collect()
        ticket_average_value = scalar(ticket_average, "value")
        product_stats = products_per_invoice.first()
        month_row = top_month.first()
        return_percentage_value = scalar(return_percentage, "percentage")

        country_lines = "\n".join(
            f"{index}. {row['Country']}: {format_number(row['country_total'])}"
            for index, row in enumerate(country_rows, start=1)
        )
        customer_id = int(customer_row["CustomerID"]) if customer_row["CustomerID"] is not None else None
        conclusions = f"""# Conclusiones - Online Retail Dataset

Resultados generados automaticamente con PySpark 3.5.6 a partir de `data/raw/online_retail.csv`.

## Respuestas

1. **Facturas totales:** {total_invoices_value:,}.
2. **Clientes unicos:** {unique_customers_value:,} (se excluyen identificadores nulos).
3. **Ingreso total neto:** {format_number(total_revenue_value)}. Se calcula como `Quantity * UnitPrice`, incluyendo devoluciones negativas.
4. **Producto mas vendido:** `{product_row['Description']}` (`{product_row['StockCode']}`), con {product_row['quantity_sold']:,} unidades positivas.
5. **Cliente con mayor volumen de compra:** `{customer_id}`, con {format_number(customer_row['customer_total'])} netos.
6. **Cinco paises que mas compran fuera de Reino Unido, por ingreso neto:**

{country_lines}

7. **Ticket promedio por factura:** {format_number(ticket_average_value)}.
8. **Referencias de producto distintas por factura:** minimo {product_stats['minimum']}, maximo {product_stats['maximum']}, promedio {format_number(product_stats['average'])}.
9. **Mes con mas ventas por ingreso neto:** mes {month_row['invoice_month']}, con {format_number(month_row['monthly_total'])}.
10. **Porcentaje de facturas con devoluciones:** {format_number(return_percentage_value)}%.

## Criterios usados

- Una factura se cuenta por `InvoiceNo` distinto.
- El ingreso neto conserva cantidades negativas para reflejar devoluciones.
- El producto mas vendido usa solo cantidades positivas.
- “Productos por factura” representa referencias `StockCode` distintas; el campo `invoice_quantity` tambien queda calculado en el ETL.
- El ranking de clientes se genera con `rank()` y se exportan los 100 primeros.

## Archivos de salida

Los resultados individuales estan en `data/output/`, un CSV por pregunta, junto con `ranking_clientes_top_100.csv`.
"""
        CONCLUSIONS_PATH.write_text(conclusions, encoding="utf-8")
        print(f"ETL completado. Resultados: {OUTPUT_DIR}")
        print(f"Conclusiones: {CONCLUSIONS_PATH}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

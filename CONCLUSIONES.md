# Conclusiones - Online Retail Dataset

Resultados generados automaticamente con PySpark 3.5.6 a partir de `data/raw/online_retail.csv`.

## Respuestas

1. **Facturas totales:** 25,900.
2. **Clientes unicos:** 4,372 (se excluyen identificadores nulos).
3. **Ingreso total neto:** 9,747,747.93. Se calcula como `Quantity * UnitPrice`, incluyendo devoluciones negativas.
4. **Producto mas vendido:** `PAPER CRAFT , LITTLE BIRDIE` (`23843`), con 80,995 unidades positivas.
5. **Cliente con mayor volumen de compra:** `14646`, con 279,489.02 netos.
6. **Cinco paises que mas compran fuera de Reino Unido, por ingreso neto:**

1. Netherlands: 284,661.54
2. EIRE: 263,276.82
3. Germany: 221,698.21
4. France: 197,403.90
5. Australia: 137,077.27

7. **Ticket promedio por factura:** 376.36.
8. **Referencias de producto distintas por factura:** minimo 1, maximo 1110, promedio 20.51.
9. **Mes con mas ventas por ingreso neto:** mes 11, con 1,461,756.25.
10. **Porcentaje de facturas con devoluciones:** 19.97%.

## Criterios usados

- Una factura se cuenta por `InvoiceNo` distinto.
- El ingreso neto conserva cantidades negativas para reflejar devoluciones.
- El producto mas vendido usa solo cantidades positivas.
- “Productos por factura” representa referencias `StockCode` distintas; el campo `invoice_quantity` tambien queda calculado en el ETL.
- El ranking de clientes se genera con `rank()` y se exportan los 100 primeros.

## Archivos de salida

Los resultados individuales estan en `data/output/`, un CSV por pregunta, junto con `ranking_clientes_top_100.csv`.

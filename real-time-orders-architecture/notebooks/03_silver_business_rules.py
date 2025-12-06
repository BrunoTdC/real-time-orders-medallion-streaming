# Databricks notebook source
# 03_silver_business_rules

%run ./00_config_paths_and_schema

from pyspark.sql.functions import *

# =================================================================
# 1. Leitura da Silver Trusted
# =================================================================

df_trusted = spark.read.format("delta").load(silver_trusted_path)

# =================================================================
# 2. Aplicação de regras de negócio
#    - Remover clientes de teste
#    - Calcular line_total (Price * Quantity)
#    - Flags de negócio (ex.: cliente UK)
# =================================================================

test_customers = ["99999", "TESTE", "12345"]  # exemplo

df_business = (
    df_trusted
    # remove clientes de teste
    .filter(~col("customer_no").isin(test_customers))
    # receita da linha
    .withColumn("line_total", col("price") * col("quantity"))
    # flag por país
    .withColumn("is_uk_customer", col("country") == lit("UNITED KINGDOM"))
    # data da venda (dia)
    .withColumn("sale_date", to_date("event_datetime"))
)

# =================================================================
# 3. Escrita da Silver Business
# =================================================================

(
    df_business
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(silver_business_path)
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver_sales_business
    USING DELTA
    LOCATION '{silver_business_path}'
""")

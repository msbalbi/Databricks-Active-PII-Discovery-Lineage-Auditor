from pyspark.sql import functions as F

# ==============================================================================
# 1. COLETA GLOBAL DE METADADOS DE TABELAS (SYSTEM TABLES / UNITY CATALOG)
# ==============================================================================
# Coleta todas as tabelas de todos os catálogos ativos (Silver, Gold, etc.)
df_tables = spark.table("system.information_schema.tables") \
    .filter(~F.col("table_schema").isin("information_schema", "billing", "access")) \
    .select(
        F.lower(F.concat_ws(".", F.col("table_catalog"), F.col("table_schema"), F.col("table_name"))).alias("Tabela_Completa"),
        F.col("table_catalog").alias("Catalogo"),
        F.col("table_schema").alias("Schema"),
        F.col("table_name").alias("Nome_Tabela"),
        F.coalesce(F.col("comment"), F.lit("FALTA COMENTÁRIO NA TABELA")).alias("Comentario_Tabela")
    )

# ==============================================================================
# 2. COLETA GLOBAL DE METADADOS DE COLUNAS/ATRIBUTOS
# ==============================================================================
# Mapeia cada coluna, seu tipo e se possui comentário preenchido
df_columns = spark.table("system.information_schema.columns") \
    .filter(~F.col("table_schema").isin("information_schema", "billing", "access")) \
    .select(
        F.lower(F.concat_ws(".", F.col("table_catalog"), F.col("table_schema"), F.col("table_name"))).alias("Tabela_Completa"),
        F.col("column_name").alias("Nome_Coluna"),
        F.col("data_type").alias("Tipo_Dado"),
        F.coalesce(F.col("comment"), F.lit("FALTA COMENTÁRIO NO ATRIBUTO")).alias("Comentario_Coluna")
    )

# ==============================================================================
# 3. UNIFICAÇÃO DOS METADADOS E ANÁLISE DE SAÚDE DA DOCUMENTAÇÃO
# ==============================================================================
df_health_check = df_tables.join(df_columns, on="Tabela_Completa", how="inner") \
    .withColumn(
        "Status_Documentacao",
        F.when(
            (F.col("Comentario_Tabela") == "FALTA COMENTÁRIO NA TABELA") & 
            (F.col("Comentario_Coluna") == "FALTA COMENTÁRIO NO ATRIBUTO"), 
            F.lit("Crítico: Sem nenhuma documentação")
        ).when(
            (F.col("Comentario_Tabela") != "FALTA COMENTÁRIO NA TABELA") & 
            (F.col("Comentario_Coluna") == "FALTA COMENTÁRIO NO ATRIBUTO"), 
            F.lit("Parcial: Tabela OK, Atributo pendente")
        ).when(
            (F.col("Comentario_Tabela") == "FALTA COMENTÁRIO NA TABELA") & 
            (F.col("Comentario_Coluna") != "FALTA COMENTÁRIO NO ATRIBUTO"), 
            F.lit("Parcial: Atributo OK, Tabela pendente")
        ).otherwise(F.lit("100% Documentado"))
    )

# ==============================================================================
# 4. EXIBIÇÃO DO RELATÓRIO EXECUTIVO DE GOVERNANÇA
# ==============================================================================
display(df_health_check.select(
    "Catalogo", 
    "Schema", 
    "Nome_Tabela", 
    "Comentario_Tabela", 
    "Nome_Coluna", 
    "Tipo_Dado", 
    "Comentario_Coluna",
    "Status_Documentacao"
).orderBy("Catalogo", "Schema", "Nome_Tabela", "Nome_Coluna"))

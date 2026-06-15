from pyspark.sql.types import StructType
from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# ==============================================================================
# CONFIGURAÇÃO DE ENTRADA (MANTÉM TODAS AS SUAS TABELAS NA ORDEM)
# ==============================================================================
VARIACOES_ALVO = []

VARIACOES_CPF_EXPANDIDO = []

TABELAS_ALVO = []

df_ordem = spark.createDataFrame([(t, i) for i, t in enumerate(TABELAS_ALVO)], ["Tabela_Original", "Ordem_Original"])

# ==============================================================================
# 1. BUSCA METADADOS DE COLUNAS GLOBAL (Coleta Absoluta em Lista)
# ==============================================================================
catalogos = list(set([t.split('.')[0] for t in TABELAS_ALVO]))
if "sandbox" not in catalogos:
    catalogos.append("sandbox")

df_meta_base = None
for cat in catalogos:
    try:
        df_temp = spark.table(f"{cat}.information_schema.columns") \
            .select(
                F.lower(F.concat_ws(".", F.col("table_catalog"), F.col("table_schema"), F.col("table_name"))).alias("Tabela_Completa_Lower"),
                F.col("column_name").alias("Coluna_Real_Meta"),
                F.col("data_type").alias("Tipo_Dado_Meta"),
                F.col("comment").alias("Comentario_Meta")
            )
        df_meta_base = df_temp if df_meta_base is None else df_meta_base.union(df_temp)
    except Exception:
        continue

df_comentarios_match = df_meta_base.filter(F.lower(F.col("Comentario_Meta")).rlike("|".join(VARIACOES_ALVO))) \
    .groupBy("Tabela_Completa_Lower") \
    .agg(F.max(F.col("Comentario_Meta")).alias("Comentario_Encontrado"))

df_meta_filtrado = df_meta_base.filter(F.lower(F.col("Coluna_Real_Meta")).substr(0, 50).isin(VARIACOES_ALVO)) \
    .join(df_comentarios_match, on="Tabela_Completa_Lower", how="full") \
    .filter((F.col("Coluna_Real_Meta").isNotNull()) | (F.col("Comentario_Encontrado").isNotNull())) \
    .groupBy("Tabela_Completa_Lower") \
    .agg(
        F.concat_ws(", ", F.collect_list("Coluna_Real_Meta")).alias("Coluna_Real"),
        F.concat_ws(", ", F.collect_list(F.when(F.col("Tipo_Dado_Meta") == "LONG", F.lit("BIGINT")).otherwise(F.col("Tipo_Dado_Meta")))).alias("Tipo_Dado"),
        F.max("Comentario_Encontrado").alias("Comentario_Encontrado")
    ).alias("meta_filtrado")

# ==============================================================================
# 2. EXTRAI LINHAGEM E FILTRA DESTINOS VIVOS
# ==============================================================================
df_lineage = spark.table("system.access.table_lineage") \
    .select(
        F.lower(F.concat_ws(".", F.col("source_table_catalog"), F.col("source_table_schema"), F.col("source_table_name"))).alias("Tabela_Original_Lineage"),
        F.concat_ws(".", F.col("target_table_catalog"), F.col("target_table_schema"), F.col("target_table_name")).alias("Tabela_Consumidora"),
        F.lower(F.concat_ws(".", F.col("target_table_catalog"), F.col("target_table_schema"), F.col("target_table_name"))).alias("Tabela_Consumidora_Lower")
    ).dropDuplicates()

df_objetos_sistema = None
for cat in catalogos:
    try:
        df_obj_temp = spark.table(f"{cat}.information_schema.tables") \
            .select(
                F.lower(F.concat_ws(".", F.col("table_catalog"), F.col("table_schema"), F.col("table_name"))).alias("Tabela_Consumidora_Viva_Lower"),
                F.col("table_type").alias("Tipo_Consumidor_Real")
            )
        df_objetos_sistema = df_obj_temp if df_objetos_sistema is None else df_objetos_sistema.union(df_obj_temp)
    except Exception:
        continue

df_lineage_completa = df_lineage.join(
    df_objetos_sistema,
    df_lineage["Tabela_Consumidora_Lower"] == df_objetos_sistema["Tabela_Consumidora_Viva_Lower"],
    how="left"
)

# ==============================================================================
# 3. MAPEAMENTO DE ESTRUTURAS DAS CONSUMIDORAS
# ==============================================================================
df_meta_consumidoras = df_meta_base \
    .select(
        F.col("Tabela_Completa_Lower").alias("Key_Completa_Lower"),
        F.col("Coluna_Real_Meta").alias("Coluna_CPF_Consumidora"),
        F.when(F.col("Tipo_Dado_Meta") == "LONG", F.lit("BIGINT")).otherwise(F.col("Tipo_Dado_Meta")).alias("Tipo_Dado_Consumidora_Real")
    ) \
    .filter(F.lower(F.col("Coluna_CPF_Consumidora")).isin(VARIACOES_CPF_EXPANDIDO))

df_meta_agrupada = df_meta_consumidoras \
    .groupBy("Key_Completa_Lower") \
    .agg(
        F.concat_ws(", ", F.collect_list("Coluna_CPF_Consumidora")).alias("Coluna_Efetiva_Destino"),
        F.concat_ws(", ", F.collect_list("Tipo_Dado_Destino_Real")).alias("Tipo_Dado_Destino_Real")
    ).alias("meta_agrupada")

# ==============================================================================
# 4. UNIFICAÇÃO ABSOLUTA COM ALIASES SEGUROS
# ==============================================================================
df_lineage_com_meta_destino = df_lineage_completa.alias("lineage").join(
    df_meta_agrupada,
    F.col("lineage.Tabela_Consumidora_Lower") == F.col("meta_agrupada.Key_Completa_Lower"),
    how="left"
).alias("lineage_final")

df_intermediario = df_ordem.alias("ordem") \
    .join(df_meta_filtrado, F.lower(F.col("ordem.Tabela_Original")) == F.col("meta_filtrado.Tabela_Completa_Lower"), how="left") \
    .join(df_lineage_com_meta_destino, F.lower(F.col("ordem.Tabela_Original")) == F.col("lineage_final.Tabela_Original_Lineage"), how="left") \
    .select(
        F.col("ordem.Ordem_Original"),
        F.col("ordem.Tabela_Original"),
        F.col("meta_filtrado.Comentario_Encontrado"),
        F.coalesce(F.col("meta_filtrado.Coluna_Real"), F.lit("-")).alias("Coluna_Real"),
        F.coalesce(F.col("meta_filtrado.Tipo_Dado"), F.lit("-")).alias("Tipo_Dado"),
        F.coalesce(F.col("lineage_final.Tabela_Consumidora"), F.lit("-")).alias("Tabela_Consumidora"),
        F.coalesce(F.col("lineage_final.Tipo_Consumidor_Real"), F.lit("-")).alias("Tipo_Consumidor"),
        
        # Consolidação da Tipagem Destino versus Origem
        F.coalesce(
            F.when(F.col("lineage_final.Tipo_Dado_Destino_Real") != "-", F.col("lineage_final.Tipo_Dado_Destino_Real")),
            F.col("meta_filtrado.Tipo_Dado"),
            F.lit("-")
        ).alias("Tipo_Dado_Consumidora"),
        
        F.coalesce(F.col("lineage_final.Coluna_Efetiva_Destino"), F.col("meta_filtrado.Coluna_Real"), F.lit("-")).alias("Coluna_Efetiva_Destino")
    ).dropDuplicates(["Tabela_Original", "Tabela_Consumidora"])

# ==============================================================================
# 5. PROCESSAMENTO DAS REGRAS DE NEGÓCIO FINAIS (CORREÇÃO CONCEITUAL)
# ==============================================================================
df_resultado_final = df_intermediario.withColumn(
    "Encontrado",
    F.when((F.col("Coluna_Real") != "-") | (F.col("Comentario_Encontrado").isNotNull()), F.lit("Sim")).otherwise(F.lit("Não"))
).withColumn(
    "Motivo_Match",
    F.when(F.col("Coluna_Real") != "-", F.lit("Nome da Coluna"))
     .when(F.col("Comentario_Encontrado").isNotNull(), F.concat(F.lit("Comentário: '"), F.col("Comentario_Encontrado"), F.lit("'")))
     .otherwise(F.lit("-"))
).withColumn(
    "Consumidora_Zeros_Esquerda",
    F.when(F.col("Tabela_Consumidora") == "-", F.lit("-"))
     .when(F.col("Tipo_Consumidor") == "-", F.lit("Objeto excluído ou temporário (Histórico de linhagem)"))
     
     # REGRA DE OURO: Se o Tipo do Destino for IGUAL ao Tipo da Origem, manteve estrutura original
     .when(F.col("Tipo_Dado_Consumidora") == F.col("Tipo_Dado"), F.lit("Sem alteração (Manteve estrutura original)"))
     
     # Análise dinâmica de truncamento caso os tipos difiram
     .otherwise(
        F.when(F.col("Tipo_Dado_Consumidora") == "STRING, STRING", F.lit("Sim (Manteve tipo String), Sim (Manteve tipo String)"))
         .when(F.col("Tipo_Dado_Consumidora") == "STRING, BIGINT", F.lit("Sim (Manteve tipo String), Não (Truncado ou Numérico)"))
         .when(F.col("Tipo_Dado_Consumidora") == "BIGINT, STRING", F.lit("Não (Truncado ou Numérico), Sim (Manteve tipo String)"))
         .when(F.col("Tipo_Dado_Consumidora") == "STRING", F.lit("Sim (Manteve tipo String)"))
         .when(F.col("Tipo_Dado_Consumidora") == "BIGINT", F.lit("Não (Truncado ou Numérico)"))
         .otherwise(
            F.when(F.col("Tipo_Dado_Consumidora").rlike("(?i)(int|long|bigint|short|double|float|decimal|numeric)"), F.lit("Não (Truncado ou Numérico)"))
             .when(F.col("Tipo_Dado_Consumidora").rlike("(?i)(string|varchar|char)"), F.lit("Sim (Manteve tipo String)"))
             .otherwise(F.lit("Verificar estrutura"))
         )
     )
).withColumn(
    "Exportado",
    F.when(F.col("Tabela_Original").rlike("(?i)(_extracao|_cdp|responsys|medallia|tickets)"), F.lit("Sim")).otherwise(F.lit("Não"))
).withColumn(
    "Tipo_Exportacao",
    F.when(F.col("Exportado") == "Sim", F.lit("CSV / API (Marketing & CRM)")).otherwise(F.lit("-"))
)

# Exibe a saída executiva limpa e validada com a regra de paridade ativa
display(df_resultado_final.select(
    "Tabela_Original", "Encontrado", "Motivo_Match", "Coluna_Real", "Tipo_Dado",
    "Tabela_Consumidora", "Tipo_Consumidor", "Tipo_Dado_Consumidora", "Coluna_Efetiva_Destino",
    "Consumidora_Zeros_Esquerda", "Exportado", "Tipo_Exportacao"
).orderBy("Ordem_Original"))
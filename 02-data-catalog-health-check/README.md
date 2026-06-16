## 🏗️ Arquitetura do Motor de Auditoria

O script é estruturado em 4 grandes etapas lógicas utilizando o poder de processamento distribuído do Spark:

1. **Coleta Global de Tabelas:** Consulta as tabelas de sistema do Unity Catalog (`system.information_schema.tables`), isolando os Schemas corporativos ativos das camadas de dados (Silver, Gold, etc.).
2. **Coleta Global de Atributos:** Mapeia cada coluna, tipo primitivo e a existência de comentários associados (`system.information_schema.columns`).
3. **Unificação e Matriz de Criticidade:** Consolida as duas visões aplicando regras de validação para classificar o status da governança em tempo real:
   * 🔴 **Crítico:** Sem nenhuma documentação (Tabela e Coluna vazias).
   * 🟡 **Parcial:** Tabela preenchida e Coluna pendente (ou vice-versa).
   * 🟢 **100% Documentado:** Contexto completo mapeado.
4. **Exibição Executiva:** Consolida a base de metadados em um DataFrame final e utiliza a engine de renderização do Databricks para ordenação e análise imediata.

---

## 🚀 Como Executar

1. Crie um novo Notebook no seu Workspace do Databricks.
2. Certifique-se de que o seu cluster possui acesso de leitura às tabelas de sistema do Unity Catalog (`system.information_schema`).
3. Cole o código do arquivo `catalog_health_check.py` em uma célula e execute.
4. Utilize os filtros interativos do próprio componente `display()` do Databricks para explorar o relatório por Catálogo ou nível de Criticidade.

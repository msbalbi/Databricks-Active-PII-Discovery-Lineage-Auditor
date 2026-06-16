# 🏛️ Databricks Unity Catalog - Active Governance & Audit Ecosystem

Este repositório reúne um ecossistema de ferramentas e motores de auditoria automatizada desenvolvidos em **PySpark**. O objetivo central é transformar a Governança de Dados e o Compliance regulatório dentro do **Databricks Unity Catalog** em indicadores técnicos, dinâmicos e mensuráveis por código, eliminando a dependência de mapeamentos manuais ou planilhas estáticas.

O ecossistema está estrategicamente dividido em dois grandes módulos de engenharia de metadados:

---

## 📂 Estrutura do Repositório

### 🔗 [01. Active PII Discovery & Lineage Auditor](./01-pii-lineage-audit)
Motor focado em **segurança, privacidade e conformidade com a LGPD**. Ele realiza uma varredura profunda nas tabelas de sistema para rastrear o ciclo de vida e o impacto de dados sensíveis (PII), tomando como exemplo prático o CPF.
* **Destaques:** Rastreamento de linhagem viva de consumo (`system.access.table_lineage`), detecção de quebra de integridade por tipagem (perda de zeros à esquerda ao converter `STRING` para `BIGINT`) e alertas de vazamento em tabelas de exportação para CRMs/APIs parceiras.

### 📊 [02. Data Catalog Health Check & Documentation Score](./02-data-catalog-health-check)
Motor focado na **saúde global, maturidade e documentação do Lakehouse**. Ele varre o `system.information_schema` para mapear a qualidade descritiva das camadas de dados (Silver, Gold, etc.).
* **Destaques:** Classificação automática do status de documentação por linhas e colunas (*"Crítico: Sem nenhuma documentação"*, *"Parcial"*, *"100% Documentado"*). Fornece uma métrica executiva ideal para comitês de dados e prepara a arquitetura para o uso confiável de IA Generativa (RAG/LLM).

---

## 🎯 Principais Dores de Negócio que este Ecossistema Resolve

* **Redução do Débito Técnico:** Mapeia de forma cirúrgica os pontos cegos de infraestrutura e tabelas criadas sem contexto.
* **Autonomia do Negócio (Self-Service BI):** Identifica os gargalos de documentação que geram chamados excessivos na TI e atrasam o time-to-market dos analistas de negócio.
* **Prontidão para IA (AI-Ready Metadata):** Garante que os metadados do Unity Catalog estejam ricos o suficiente para que agentes de IA (como o Databricks Assistant) trabalhem sem alucinar.
* **Auditoria por Squads/Tribos:** Permite isolar os indicadores por Catálogo e Schema, evidenciando quais times estão seguindo as boas práticas de governança.

---

## 🚀 Requisitos Gerais

* Workspace Databricks com o **Unity Catalog** habilitado.
* Ativação das **System Tables** (`system.information_schema` e `system.access`).
* Cluster ativo com suporte a queries em PySpark/SparkSQL.

Para instruções específicas de execução e arquitetura de cada motor, acesse os READMEs internos de cada diretório.

---
💡 *Este ecossistema foi desenvolvido para demonstrar a aplicação prática de engenharia de dados sênior voltada para arquiteturas modernas de governança ativa e DataOps.*

# Create the README.md content and code block content
readme_content = """# Databricks Active PII Discovery & Lineage Auditor

Este repositório contém um motor de auditoria automatizada desenvolvido em **PySpark** focado em **Data Discovery Ativo e Análise de Impacto de Linhagem** dentro do ecossistema Databricks utilizando o **Unity Catalog**.

O script realiza uma varredura profunda no `information_schema.columns` e cruza os metadados com as tabelas de sistema de linhagem ativa (`system.access.table_lineage`), permitindo rastrear o ciclo de vida, a paridade de tipos e as exportações secundárias de informações sensíveis (PII - *Personally Identifiable Information*), tomando como exemplo o **CPF** e suas diversas variações de nomenclatura operacionais no varejo/CRM.

## 🚀 Principais Perguntas que o Projeto Responde

1. **Onde estão os dados sensíveis? (Active Data Discovery)**
   Identifica quais tabelas das camadas Silver e Gold possuem armazenamento de documentos confidenciais, mapeando variações dinâmicas de colunas (como `govid`, `customer_id`, `seller_document_number`, etc.) e analisando comentários e metadados.
2. **Qual é a integridade do dado trafegado? (Paridade de Tipos)**
   Mapeia se o documento sofreu conversão implicando perda de integridade (ex: alteração de `STRING` para `BIGINT` causando a perda de zeros à esquerda). O script aponta o status dinâmico para evitar quebras em integrações e relatórios de CRM.
3. **Para onde o dado sensível está vazando? (Linhagem Ativa)**
   Rastreia de forma cirúrgica as tabelas consumidoras finais que ainda estão vivas e ativas no sistema, descartando metadados históricos de objetos temporários já excluídos.
4. **Existem exportações para sistemas terceiros? (Data Privacy Exfiltration)**
   Sinaliza imediatamente se o dado confidencial está caindo em tabelas de extração destinadas a integrações com parceiros via APIs ou arquivos (ex: Salesforce Responsys, Medallia, Zendesk, etc.).

---

## 🛠️ Arquitetura do Motor de Auditoria

O script é estruturado de forma linear e performática, dividido em 5 fases principais:

# Financial Market Data Lakehouse

Projeto de engenharia de dados end-to-end para ingestão, armazenamento e processamento de dados financeiros de mercado usando AWS S3, Databricks, Delta Lake e GitHub.

## Objetivo
Construir um pipeline de dados em arquitetura Medallion (Bronze, Silver, Gold), consumindo dados de mercado via API e disponibilizando métricas analíticas para dashboards.

## Tecnologias
- Python
- AWS S3
- Databricks
- Delta Lake
- PySpark
- SQL
- GitHub

## Status do projeto
- [x] Estrutura inicial criada
- [x] Bucket S3 criado
- [x] Pastas raw / bronze / silver / gold criadas
- [x] Ingestão inicial via API
- [ ] Processamento Bronze
- [ ] Processamento Silver
- [ ] Processamento Gold
- [ ] Orquestração com Databricks Jobs
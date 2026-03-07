# Financial Market Data Lakehouse

Projeto de engenharia de dados end-to-end para ingestão, armazenamento e processamento de dados financeiros utilizando arquitetura Lakehouse com AWS S3, Databricks e Delta Lake.

O pipeline consome dados da API Alpha Vantage, armazena dados brutos em um Data Lake na AWS e processa os dados utilizando a arquitetura Medallion (Bronze, Silver, Gold) no Databricks.

---

# Arquitetura

Alpha Vantage API  
        ↓  
Python Ingestion  
        ↓  
AWS S3 (Raw Data Lake)  
        ↓  
Databricks Bronze Layer  
        ↓  
Databricks Silver Layer  
        ↓  
Databricks Gold Layer  
        ↓  
Analytics / Dashboards

---

# Tecnologias Utilizadas

- Python
- PySpark
- Databricks
- Delta Lake
- AWS S3
- SQL
- GitHub

---

# Estrutura do Projeto
financial-market-lakehouse
│
├── notebooks
│ ├── 01_bronze_alpha_vantage.py
│ ├── 02_silver_alpha_vantage.py
│ └── 03_gold_market_analytics.py
│
├── src
│ └── ingestion
│ ingest_market_data.py
│
├── configs
├── requirements.txt
├── README.md
└── .gitignore


---

# Camadas do Pipeline

## Bronze Layer
Responsável por transformar os dados brutos da API em uma estrutura tabular.

Transformações:
- parsing do JSON da API
- extração das séries temporais
- conversão de tipos de dados
- padronização de datas
- inclusão de metadados de ingestão

Tabela gerada:
financial_market.bronze.alpha_vantage_daily


---

## Silver Layer
Responsável pela limpeza e padronização dos dados.

Transformações:
- remoção de duplicatas
- validação de dados nulos
- filtragem de preços inválidos
- cálculo de retorno diário
- cálculo da amplitude de preço

Tabela gerada:
financial_market.silver.alpha_vantage_daily_clean


---

## Gold Layer
Camada analítica para consumo em dashboards e análises financeiras.

Métricas calculadas:

- média móvel de 7 dias
- média móvel de 30 dias
- volatilidade
- retorno acumulado
- ranking diário de ativos

Tabela gerada:
financial_market.gold.market_analytics


---

# Status do Projeto

- [x] Estrutura inicial criada
- [x] Bucket S3 criado
- [x] Data Lake organizado
- [x] Ingestão via API Alpha Vantage
- [x] Camada Bronze implementada
- [x] Camada Silver implementada
- [x] Camada Gold implementada
- [x] Orquestração com Databricks Jobs
- [ ] Dashboard analítico


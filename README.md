# Wikimedia Analytics Pipeline

Este projeto implementa um pipeline de processamento de dados em streaming utilizando dados do **Wikimedia Recent Changes**.

A solução foi desenvolvida com **Databricks** e **Apache Spark Structured Streaming**, com foco em ingestão contínua, transformação de dados, validação, agregações e geração de saídas em formato Parquet.

## Fonte de Dados

A fonte de dados utilizada é o stream público de mudanças recentes da Wikimedia:

https://stream.wikimedia.org/v2/stream/recentchange

Esse endpoint disponibiliza eventos em tempo real por meio do padrão Server-Sent Events (SSE), representando alterações recentes em projetos Wikimedia, como Wikipedia, Wikidata e Wikimedia Commons.

## Objetivo

O objetivo deste projeto é construir um pipeline de streaming capaz de:

- ingerir eventos em tempo real da Wikimedia;
- armazenar os eventos brutos em formato JSON Lines;
- processar os dados com Apache Spark Structured Streaming;
- converter a saída de JSON para Parquet;
- aplicar regras de validação e filtragem;
- gerar agregações analíticas;
- aplicar Window Function para análise temporal;
- documentar uma proposta de deploy utilizando Databricks Jobs.

  
## Arquitetura

```text
Wikimedia Recent Changes Stream
        ↓
Raw JSON Events
        ↓
Bronze Parquet
        ↓
Silver Validated Parquet
        ↓
Gold Aggregations
        ↓
Window Aggregations
```

## Tecnologias Utilizadas
- Databricks
- Apache Spark Structured Streaming
- Python
- Unity Catalog Volume
- Parquet
- Wikimedia Recent Changes Stream
  
## Camadas do Pipeline

### Raw
Armazena os eventos originais capturados do stream da Wikimedia em formato JSON Lines.

### Bronze
Lê os eventos brutos da camada Raw com Spark Structured Streaming e grava os dados em formato Parquet.

### Silver
Lê os dados da camada Bronze em modo streaming, aplica validações, filtros e enriquecimentos, e grava os eventos tratados em formato Parquet.

### Gold
Gera agregações analíticas a partir dos dados validados, incluindo:

- eventos por wiki;
- eventos por tipo;
- usuários mais ativos;
- eventos por wiki em janelas temporais.

## Evidências de Streaming

O projeto utiliza Spark Structured Streaming por meio de:

- spark.readStream;
- writeStream;
- checkpoints;
- processamento em micro-batches;
- trigger(availableNow=True).

Embora o notebook utilize availableNow=True para tornar a execução finita e reprodutível, o processamento é realizado com Spark Structured Streaming, mantendo leitura incremental, controle por checkpoint e execução em micro-batches.

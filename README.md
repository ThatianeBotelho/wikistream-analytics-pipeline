# WikiStream Analytics Pipeline

Este projeto implementa um pipeline de processamento de dados em streaming utilizando dados do **Wikimedia Recent Changes**.

A solução foi desenvolvida com **Databricks** e **Apache Spark Structured Streaming**, com foco em ingestão contínua, transformação de dados, validação, agregações e geração de saídas em formato Parquet.

---

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
- aplicar regras de validação, filtragem e enriquecimento;
- gerar agregações analíticas;
- aplicar Window Functions para análise temporal;
- utilizar Watermarking para controle de janelas em dados de streaming;
- documentar uma proposta de execução e deploy utilizando Databricks.

  
## Arquitetura

```text
Wikimedia Recent Changes Stream (SSE)
        ↓
Raw: JSON Lines (Unity Catalog Volume)
        ↓
Bronze: Parquet (conversão de formato via readStream/writeStream)
        ↓
Silver: Parquet Validado (filtros, transformações, schema enriquecido)
        ↓
Gold: Parquet Agregado (Window Functions + Watermarking)
```

## Tecnologias Utilizadas

| Camada | Tecnologia |
|---|---|
| Plataforma | Databricks |
| Processamento | Apache Spark Structured Streaming |
| Armazenamento | Unity Catalog Volume (Managed) |
| Ingestão | Python `requests` + Server-Sent Events (SSE) |
| Formato de saída | Apache Parquet |
| Linguagem | Python (PySpark) |

  
## Camadas do Pipeline

### Raw (Ingestão)
Armazena os eventos originais capturados do stream da Wikimedia em formato **JSON Lines**.
A ingestão consome o endpoint **Wikimedia Recent Changes** via **Server-Sent Events (SSE)**, utilizando `requests` com `stream=True`. Essa abordagem mantém a conexão HTTP aberta e permite ler os eventos incrementalmente por meio do método `response.iter_lines()`.

### Bronze
Lê os arquivos JSON da camada Raw em modo streaming utilizando `spark.readStream`, com schema explícito, e grava os dados em formato Parquet com `writeStream`.

### Silver
Lê os dados da camada Bronze em modo streaming, aplica validações, filtros e enriquecimentos, e grava os eventos tratados em formato Parquet.

### Gold
Gera agregações analíticas a partir dos dados validados da Silver. 
As agregações utilizam **Window Functions de 5 minutos** e **Watermarking de 10 minutos**, permitindo análises temporais sobre os eventos processados.
Cada agregação possui checkpoint próprio, garantindo controle independente de progresso e retomada incremental.

| Agregação | Caminho | Dimensão | Métricas |
|---|---|---|---|
| Impacto de Conteúdo | `gold/aggregations_parquet/window_impact` | `wiki` | `total_edits`, `total_content_impact` |
| Engajamento de Usuários | `gold/aggregations_parquet/user_activity` | `user`, `bot` | `total_events` |
| Perfil de Eventos | `gold/aggregations_parquet/event_types` | `wiki`, `type` | `total_events` |

Antes da aplicação do `withWatermark`, o pipeline remove registros com timestamps nulos, garantindo consistência no cálculo das janelas temporais.

## Evidências de Streaming

O projeto utiliza Apache Spark Structured Streaming por meio de:

- uso de `spark.readStream` para leitura incremental;
- uso de `writeStream` para gravação dos dados processados;
- uso de checkpoints para controle de progresso;
- processamento em micro-batches;
- uso de `trigger(availableNow=True)`;
- uso de `withWatermark` nas agregações temporais;
- uso de Window Functions para análise em janelas de tempo.

Embora o notebook utilize `availableNow=True` para tornar a execução finita e reprodutível, o processamento mantém a semântica de Structured Streaming, com leitura incremental, controle por checkpoint e execução em micro-batches.


## Estrutura do Repositório

```text
wikimedia-analytics-pipeline/
│
├── wikimedia-stream-pipeline-final.ipynb   # Notebook principal do pipeline
├── README.md                               # Documentação do projeto
├── requirements.txt                        # Dependências Python do projeto
└── .gitignore                              # Arquivos e pastas ignorados pelo Git
```

## Como Executar

O notebook foi desenvolvido e executado no **Databricks**, com acesso a um **Unity Catalog**.

### Passos de execução

1. Importar o notebook no Databricks Workspace.
2. Anexar o notebook a um cluster com Spark 3.x ou superior.
3. Executar as células em ordem.
4. O setup inicial cria o schema, o volume e os diretórios necessários.
5. A variável `RESET_PIPELINE_OUTPUTS = True` pode ser utilizada para limpar execuções anteriores antes de iniciar uma nova execução.
6. Validar as saídas nas camadas Raw, Bronze, Silver e Gold.


## Autoria

Projeto desenvolvido em coautoria por:

<table>
  <tr>
      <td align="center">
      <img style="border-radius: 50%;" 
           src="https://avatars.githubusercontent.com/ThatianeBotelho" 
           width="100px;" 
           alt="Thatiane Botelho"/>
      <br/>
      <b>Thatiane Botelho</b>
      <br/>
      <a href="https://github.com/ThatianeBotelho">GitHub</a>
    </td>
    <td align="center">
      <img style="border-radius: 50%;" 
           src="https://avatars.githubusercontent.com/tatiane-ss" 
           width="100px;" 
           alt="Tatiane Silva"/>
      <br/>
      <b>Tatiane Silva</b>
      <br/>
      <a href="https://github.com/tatiane-ss">GitHub</a>
    </td>    
    <td align="center">
      <img style="border-radius: 50%;" 
           src="https://avatars.githubusercontent.com/vivianecorrea" 
           width="100px;" 
           alt="Viviane Corrêa"/>
      <br/>
      <b>Viviane Corrêa</b>
      <br/>
      <a href="https://github.com/vivianecorrea">GitHub</a>
    </td>
  </tr>
</table>

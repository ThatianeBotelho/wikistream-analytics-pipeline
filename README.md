# WikiStream Analytics Pipeline

This project implements a stream processing pipeline using Wikimedia Recent Changes data.

The pipeline was developed as part of the Stream Processing Pipelines course in the MBA program and uses Databricks with Apache Spark Structured Streaming.

## Data Source

The data source is the Wikimedia Recent Changes stream:

https://stream.wikimedia.org/v2/stream/recentchange

This endpoint provides real-time events using Server-Sent Events (SSE), representing recent changes across Wikimedia projects such as Wikipedia, Wikidata and Wikimedia Commons.

## Objective

The objective of this project is to build a stream pipeline that:

- Ingests real-time events from Wikimedia;
- Stores raw events in JSON Lines format;
- Processes data using Apache Spark Structured Streaming;
- Converts the output from JSON to Parquet;
- Applies validation and filtering rules;
- Generates analytical aggregations;
- Applies a Window Function for temporal analysis;
- Documents a deployment proposal using Databricks Jobs.

## Architecture

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

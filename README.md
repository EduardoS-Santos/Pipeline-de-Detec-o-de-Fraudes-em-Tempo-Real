# Pipeline de Detecção de Fraudes em Tempo Real (100k TPS)

Este projeto consiste em uma arquitetura de Big Data voltada para a ingestão, classificação e persistência distribuída de transações financeiras em altíssima escala. O sistema processa dados brutos na taxa de 100.000 eventos por segundo, identifica padrões de fraude em tempo real e armazena os eventos segregados em formato colunar otimizado (Apache Parquet) no HDFS.

---

## Arquitetura do Sistema

```text
                               ┌──────────────────────────────────────────────────────────┐
                               │                    KAFKA CLUSTER                         │
                               │                                                          │
┌──────────────┐  100k TPS     │  ┌───────────────────┐        ┌───────────────────────┐  │
│  Java App    │───────────────┼─►│ transacoes.brutas │───────┐│    transacoes.ok      │──┼──┐
│  (Producer)  │ (Batch & LZ4) │  | (Partiçoes: 12)   |        ││ (Partiçoes: 12)  │   │  │
└──────────────┘               │  └───────────────────┘       │└───────────────────────┘  │  │
                               └──────────────────────────────┼───────────────────────────┘  │
                                                              │                              │
                                                              ▼                              │
                                                   ┌─────────────────────┐                   │
                                                   │   Fraud Routers     │                   │
                                                   │  (Worker Cluster)   │                   │
                                                   └─────────────────────┘                   │
                                                              │                              │
                                                              ▼                              │
                               ┌──────────────────────────────────────────────────────────┐  │
                               │  ┌───────────────────────┐                               │  │
                               │  │   transacoes.fraud    │◄──────────────────────────────┘  │
                               │  │ (Partiçoes: 12)  │                                  │
                               │  └───────────────────────┘                                  │
                               └──────────────────────────────┬───────────────────────────┘
                                                              │
                                    ┌─────────────────────────┴────────────────────────┐
                                    ▼                                                  ▼
                      ┌───────────────────────────┐                      ┌───────────────────────────┐
                      │      HDFS Writer OK       │                      │     HDFS Writer Fraud     │
                      │  (Batch: 500k / 5 min)    │                      │   (Batch: 10k / 1 min)    │
                      └─────────────┬─────────────┘                      └─────────────┬─────────────┘
                                    │                                                  │
                                    └─────────────────────────┬────────────────────────┘
                                                              ▼
                                                   ┌─────────────────────┐
                                                   │     HDFS Cluster    │
                                                   │  (NameNode/DataNode)│
                                                   │  (Arquivos Parquet) │
                                                   └─────────────────────┘
```

---

## Tecnologias Utilizadas

* **Linguagens:** Java (Gerador de Alta Vazão / Producer) e Python 3.11+ (Routers e HDFS Writers)
* **Mensageria e Streaming:** Apache Kafka
* **Armazenamento Distribuído:** Hadoop HDFS
* **Formatos e Compressão:** Apache Parquet com compressão Snappy / LZ4
* **Orquestração e Mapeamento:** Docker e Docker Compose

---

## Componentes do Pipeline

### 1. Ingestão de Dados (Java Producer)
* **Função:** Simular e gerar a entrada massiva de 100.000 transações por segundo.
* **Otimizações:**
  * Envio assíncrono e multithreaded.
  * Agrupamento por lotes na rede (`batch.size` e `linger.ms`) para maximizar a vazão.
  * Compressão de payload (`snappy`/`lz4`) antes de enviar ao Kafka.
* **Tópico de Destino:** `transacoes.brutas`

### 2. Motor de Classificação e Roteamento (Fraud Routers)
* **Função:** Consumir do tópico bruto, aplicar regras e modelos de detecção de fraude e separar as mensagens em tópicos distintos.
* **Comportamento:**
  * Transações legítimas são publicadas em `transacoes.ok`.
  * Transações suspeitas ou fraudulentas são publicadas em `transacoes.fraud`.
* **Escalabilidade:** Dimensionável horizontalmente via `CONSUMER_GROUP_ID` unificado, permitindo paralelismo equivalente ao número de partições do Kafka.

### 3. Persistência Otimizada no HDFS (HDFS Writers)
Consumidores Python dedicados a estruturar os dados e gravá-los no HDFS, solucionando o problema de múltiplos arquivos pequenos (*Small Files Problem*).

* **`hdfs-writer-ok` (Alto Volume - Transações Legítimas):**
  * **Estratégia:** Prioriza grande compactação para otimizar leitura analítica.
  * **Critério de Flush:** 500.000 registros OU 300 segundos (5 minutos).
  * **Formato:** Apache Parquet com compressão Snappy.
  * **Estrutura de Diretório:** `/data/transacoes_ok/date_partition=YYYY-MM-DD/part_<timestamp>.parquet`

* **`hdfs-writer-fraud` (Baixo Volume - Transações Suspeitas):**
  * **Estratégia:** Prioriza menor latência para atualização rápida de alertas e auditoria.
  * **Critério de Flush:** 10.000 registros OU 60 segundos (1 minuto).
  * **Formato:** Apache Parquet com compressão Snappy.
  * **Estrutura de Diretório:** `/data/transacoes_fraud/date_partition=YYYY-MM-DD/part_<timestamp>.parquet`

---

## Dimensionamento e Performance (Benchmarking)

| Métrica | Produtor Java | Routers (Worker Cluster) | Escritores HDFS |
| :--- | :--- | :--- | :--- |
| **Vazão Alvo** | 100.000 msgs/s | 100.000 msgs/s | ~100.000 msgs/s (agregadas) |
| **Escala Recomendada** | 1 a 2 instâncias | 4 a 6 instâncias | 4 (OK) + 2 (Fraud) |
| **Partições Kafka(min)** | - | 12 partições | 12 partições |
| **Formato de Saída** | JSON em Trânsito | JSON em Trânsito | Parquet (Snappy) |

---

## Como Executar a Solução

### 1. Subir a Infraestrutura Base (Kafka + HDFS)
```bash
docker compose up -d kafka namenode datanode
```

### 2. Subir o Pipeline Processador em Escala
Para garantir o consumo e ingestão de 100k TPS sem atraso (*lag*), suba o ecossistema completo com as instâncias multiplicadas:

```bash
docker compose up -d --scale fraud-router=4 --scale hdfs-writer-ok=4 --scale hdfs-writer-fraud=2
```

---

## Estrutura de Arquivos no HDFS

O armazenamento final no HDFS fica organizado por partição diária, pronto para ser lido por qualquer engine de leitura externa:

```text
/data/
├── transacoes_ok/
│   └── date_partition=2026-09-12/
│       ├── part_1789186570087.parquet
│       ├── part_1789186870775.parquet
│       └── ...
└── transacoes_fraud/
    └── date_partition=2026-09-12/
        ├── part_1789186815179.parquet
        ├── part_1789186875328.parquet
        └── ...
```

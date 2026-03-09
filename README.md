# 🚀 CDC Full Stack Pipeline

## 📋 Descrição

Este projeto é uma implementação de um pipeline de **Change Data Capture (CDC)** utilizando tecnologias modernas para ingestão e processamento de dados em tempo real. O objetivo é capturar mudanças em um banco de dados PostgreSQL, processá-las via Apache Spark e distribuí-las para armazenamento analítico e de consulta rápida.

Além do pipeline de dados, o projeto inclui uma aplicação Full Stack:

- **Painel de Controle:** Para visualização e configuração do CDC.
- **Dashboard:** Para consumo e exibição dos dados em tempo real processados pelo Spark.

## 🏗️ Arquitetura e Fluxo de Dados

1. **Origem:** PostgreSQL 17 (com replicação lógica ativada).
2. **Ingestão (CDC):** Kafka Connect + Debezium capturam as mudanças e publicam no Apache Kafka (KRaft).
3. **Processamento:** Apache Spark consome os tópicos, realiza as transformações e orquestra a saída.
4. **Armazenamento:**
   - **Delta Lake (MinIO):** Armazenamento estruturado na arquitetura Medallion (`raw`, `silver`, `gold`).
   - **Camada de Serviço (Redis):** Cache de métricas em tempo real para consumo do Dashboard.

---

## ⚡ Como Executar

### 1. Subindo a Infraestrutura (Docker)

Primeiro, inicie todos os serviços definidos no Docker Compose:

```bash
docker compose up -d
```

Nota: Aguarde alguns instantes para que todos os serviços fiquem saudáveis (o MinIO criará os buckets e o Spark baixará os JARs necessários automaticamente).

### 2. Registrando o Conector Debezium

Com o Kafka Connect rodando, envie a configuração do conector para iniciar a captura do PostgreSQL:
Nota: Se estiver em ambiente windows, use o git bash para rodar o comando abaixo.

```bash
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
localhost:8083/connectors/ \
-d @connectors/debezium-postgres.json
```

### 3. Executando o Processador Spark

Para rodar o job do Spark, você precisa acessar o container do Spark Master ou submeter o script diretamente:

```bash
# Limpe a pasta de checkpoint (se houver execuções anteriores)
docker exec -it cdc-spark-master rm -rf /tmp/checkpoints/

# Inicie o job do Spark
docker exec -it cdc-spark-master /opt/spark-jobs/run.sh jobs/emprestimos/stream.py
```

### 4. Executando o Backend Dashboard

Deve rodar o seguinte comando dentro da pasta backend/dashboard/api:

```bash
uvicorn app.main:app --reload --port 8000
```

### 5. Executando o Painel Full Stack

...

## 🌐 Portas de acesso

| UI / Serviço           | URL                   |
| :--------------------- | :-------------------- |
| **Kafka UI**           | http://localhost:8080 |
| **Kafka Connect REST** | http://localhost:8083 |
| **Schema Registry**    | http://localhost:8081 |
| **Spark Master UI**    | http://localhost:8082 |
| **Spark Worker UI**    | http://localhost:8085 |
| **MinIO Console**      | http://localhost:9001 |
| **MinIO S3 API**       | http://localhost:9000 |
| **Redis Insight**      | http://localhost:5540 |
| **PostgreSQL**         | `localhost:5432`      |
| **Redis**              | `localhost:6379`      |

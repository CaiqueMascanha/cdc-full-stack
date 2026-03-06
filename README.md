# 🚀 CDC Full Stack

## ⚡ Execução

### Apache Spark

Para rodar o processador Spark, é necessário estar dentro do container. Execute o comando:

1. fazer um post para: http://localhost:8083/connectors contando o body de: debezium-postgres.json

2. Limpe a pasta de checkpoint:

```bash
rm -rf /tmp/checkpoints/
```

3. Inicie o Spark:

```bash
/opt/spark-jobs/run.sh cdc_processor.py
```

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

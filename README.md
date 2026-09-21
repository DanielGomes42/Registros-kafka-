# GeoSapiens — Ingestão de CSV em Larga Escala

Aplicação para importar e analisar arquivos CSV com milhões de transações, sem carregar o arquivo inteiro na memória e sem renderizar todos os registros no navegador.

## Tecnologias

- Java 21 e Spring Boot
- React e JavaScript
- PostgreSQL
- Apache Kafka
- Docker Compose
- Spring Boot Actuator e Micrometer
- Datadog opcional

## Arquitetura

```text
CSV → API Spring Boot → Kafka → Consumer em lote → PostgreSQL
                                      ↓
                              Dashboard React
1. A API recebe o CSV e cria um job de importação.
2. O arquivo é lido em streaming, uma linha por vez.
3. Cada registro válido é publicado no tópico Kafka transactions-import.
4. O consumer recebe mensagens em lote e persiste no PostgreSQL.
5. O React acompanha o progresso por polling e consulta dados paginados ou agregados.
Como executar
Pré-requisito
Tenha o Docker Desktop instalado e em execução.
Subir o projeto
docker compose up --build
Abra http://localhost:5174.
O Docker Compose sobe quatro serviços:
- web: frontend React servido pelo Nginx;
- api: backend Java Spring Boot;
- db: banco PostgreSQL;
- kafka: fila de mensagens da importação.
Para encerrar os serviços:
docker compose down
Para apagar também os dados locais do banco:
docker compose down -v
CSV de teste
Baixe um arquivo de teste no Google Drive.
Formato esperado:
occurred_at,category,amount,source
2025-01-10T12:00:00Z,food,45.90,mobile
Para gerar um CSV com 1 milhão de linhas:
node scripts/generate-csv.mjs 1000000 > transactions.csv
Para gerar exatamente 11 milhões de registros:
node scripts/generate-csv.mjs 11000000 > C:\Users\Dan\Downloads\transactions-11m.csv
A importação de 11 milhões não é automática. Antes de enviar o arquivo, confirme se há espaço suficiente no banco e no disco do Docker.
Fluxo da importação
- POST /api/imports recebe o CSV como multipart/form-data e retorna 202 Accepted com o ID do job.
- A API usa BufferedReader e Apache Commons CSV para ler uma linha por vez.
- Cada transação válida é publicada no Kafka.
- O consumer Kafka usa jdbc.batchUpdate para inserir várias linhas por operação no PostgreSQL.
- O React consulta o status do job a cada segundo e mostra linhas lidas, publicadas, persistidas e inválidas.
Endpoints principais
Endpoint	Descrição
POST /api/imports	Envia um CSV e inicia um job de importação.
GET /api/imports/{id}	Retorna status e progresso da importação.
GET /api/transactions?size=50&cursor=...&year=2025&month=12	Lista transações paginadas e filtradas.
GET /api/aggregation-years	Retorna os anos existentes para o seletor.
GET /api/aggregations?year=2025	Retorna soma e quantidade por mês e categoria.
GET /api/capacity	Retorna uso e capacidade estimada do banco.


Decisões de desempenho
Memória
O CSV é processado em streaming. O backend lê um registro por vez e não mantém uma lista com todo o arquivo em memória, evitando risco de Out Of Memory.
Kafka e persistência
Kafka desacopla a leitura do arquivo da escrita no banco. O consumer insere dados em lote, reduzindo chamadas ao PostgreSQL.
Em produção, o tópico Kafka deve ter retenção, partições, autenticação e idempotência configuradas.
Consultas
A tabela possui índice por occurred_at e id para paginação por cursor, além de índice composto por category e occurred_at para filtros e agregações.
A API retorna no máximo 200 registros por requisição.
Frontend
O React nunca carrega todas as transações. Ele recebe uma página de registros por vez ou somente dados agregados para os cards e a visualização por mês e categoria.
Capacidade e observabilidade
Variável	Função
DB_STORAGE_LIMIT_GB	Limite configurado de armazenamento do banco.
DB_ALERT_PERCENT	Percentual para alerta de capacidade.
AVG_ROW_BYTES_ESTIMATE	Estimativa de bytes médios por registro.
DATADOG_ENABLED	Ativa a exportação opcional para Datadog.
DD_API_KEY	Chave da API Datadog. Nunca registre no repositório.


A capacidade é uma estimativa. O tamanho real do banco vem de pg_database_size, mas índices, WAL, espaço do Docker e retenção do Kafka também consomem armazenamento.
Métricas locais:
- http://localhost:8081/actuator/metrics
- http://localhost:8081/actuator/prometheus
Testes
O projeto possui testes para cálculo de capacidade e parsing de CSV.
Antes de executar uma carga grande, valide o fluxo com um CSV pequeno.

Depois execute:

```powershell
git add README.md
git commit -m "docs: improve project documentation"
git push

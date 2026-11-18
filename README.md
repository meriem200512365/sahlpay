# SahlPay – Plateforme Big Data (Écosystème Big Data 2, EMSI 5A)

Preuve de concept d'une chaîne analytique pour le portefeuille mobile fictif **SahlPay.ma** :
suivi des paiements en quasi temps réel et analyse de structures inhabituelles (appareils partagés,
transferts circulaires, comptes collecteurs).

```
producer.py → Kafka → Spark Structured Streaming → MinIO (Bronze → Silver → Gold) → GraphX / GraphFrames
```

## Équipe
- Meriem – <prénom NOM du binôme>

## Avancement
| Partie | Contenu | Statut |
|---|---|---|
| 1 | Cluster Docker, SparkSession | ✅ |
| 2 | Batch SparkCore (RDD) | ⏳ |
| 3 | Spark SQL et DataFrames | ⏳ |
| 4 | Structured Streaming depuis Kafka | ⏳ |
| 5 | Pipeline de production (Silver, Gold Delta) | ⏳ |
| 6 | GraphX et GraphFrames | ⏳ |

## Prérequis
- Docker Desktop (WSL2 sous Windows) avec au moins 8 Go de RAM alloués
- Docker Compose v2
- Git
- Python 3.12 (uniquement pour `producer.py`)

## Versions
| Composant | Version |
|---|---|
| Apache Spark | 3.5.5 |
| Kafka (KRaft) | 3.7.0 |
| Stockage objet S3 | Silo (fork communautaire de MinIO) `RELEASE.2026-08-06T00-00-00Z` |
| Scala | 2.12 |

Les images officielles de MinIO ayant été retirées de Docker Hub, on utilise le fork compatible S3 `pgsty/silo`.

## Lancement
```bash
git clone <url-du-depot>
cd sahlpay-bigdata
docker compose up -d --build
docker compose ps
```

Créer le topic Kafka :
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic transactions \
  --partitions 3 --replication-factor 1 --bootstrap-server kafka:19092
```

## Interfaces
| Service | URL |
|---|---|
| Spark Master | http://localhost:8080 |
| Spark UI (Driver) | http://localhost:4040 |
| Jupyter | http://localhost:8888/?token=sahlpay |
| Console MinIO | http://localhost:9001 (sahlpay / sahlpay123) |

Les identifiants ci-dessus sont locaux et sans valeur.

## Arrêt
```bash
docker compose stop      # conserve les données
docker compose down      # supprime les conteneurs (et les données Kafka)
```

## Structure
```
├── docker-compose.yml
├── .env                 versions et identifiants locaux
├── jupyter/Dockerfile   Jupyter + PySpark aligné sur le cluster
├── notebooks/           partie1_cluster.ipynb, ...
├── data/                producer.py, merchants.json
├── report/              rapport PDF
└── screenshots/         captures de preuve
```
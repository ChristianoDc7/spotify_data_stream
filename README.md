# SPOTIFY — Plateforme de streaming musical distribuée

> **Formation Data & IA — Master 1 | 35 heures | Groupes de 3-4**

Vous allez construire **SPOTIFY**, une plateforme de streaming musical complète inspirée de la vraie. De l'ingestion du catalogue jusqu'à la détection de fraude en temps réel, en passant par un réseau peer-to-peer de distribution des morceaux.

Ce projet se construit **brique par brique sur 5 jours**. Chaque livrable s'appuie sur le précédent. À la fin, votre groupe disposera d'une plateforme opérationnelle — et vous interconnecterez vos instances avec celles des autres groupes pour former un véritable écosystème musical distribué.

---

## Ce que vous allez construire

### Architecture Phase 1 (implémentée)

```mermaid
graph TD
    SIM["🎵 Simulateur P2P<br/>(src/p2p_simulator)"]

    subgraph Redis["Redis :6380"]
        RL["listening_events_queue LIST"]
        RP["p2p_network_events_queue LIST"]
    end

    subgraph Airflow["Apache Airflow :8080"]
        DAG1["streaming_events_pipeline<br/>⏱ toutes les 5 min"]
        DAG2["catalog_ingestion_pipeline<br/>⏱ quotidien 02h00"]
        DAG3["aggregation_pipeline<br/>⏱ quotidien 04h00"]
        DAG4["dlq_reprocessing_pipeline<br/>⏱ toutes les heures"]
    end

    subgraph Postgres["PostgreSQL :5433"]
        T1["listening_events"]
        T2["tracks / artists / albums"]
        T3["daily_streams / artist_stats"]
        T4["dead_letter_events (DLQ)"]
    end

    subgraph MinIO["MinIO :9001"]
        B1["spotify-parquet/<br/>listening_events/date=/hour=/<br/>.parquet"]
        B2["labels-raw/<br/>*.json"]
    end

    SIM -->|lpush| RL
    SIM -->|lpush| RP

    DAG1 -->|lrange + delete| RL
    DAG1 -->|lrange + delete| RP
    DAG1 -->|upsert| T1
    DAG1 -->|écriture Parquet| B1
    DAG1 -->|invalides| T4

    DAG2 -->|lecture JSONs| B2
    DAG2 -->|upsert| T2

    DAG3 -->|ExternalTaskSensor| DAG1
    DAG3 -->|GROUP BY| T1
    DAG3 -->|upsert| T3

    DAG4 -->|pending events| T4
    DAG4 -->|réinjection| T1
```

### Architecture cible Phase 2 (Kafka + Spark)

```
Sources ──► Kafka topics ──► Spark Streaming ──► PostgreSQL / Redis
              │                                         │
              └──► Airflow DAGs (batch) ────────────────┘
                                                        │
                                              MinIO (Parquet + checkpoints)
```

| Couche | Technologie | Ce que vous implémentez |
|--------|-------------|------------------------|
| Orchestration batch | Apache Airflow 2.x | 5 DAGs + 2 ponts batch/streaming |
| Messaging | Apache Kafka 3.x (KRaft) | 6 topics internes + 3 inter-groupes |
| Streaming | Spark 3.5+ Structured Streaming | 3 jobs temps réel |
| Base de données | PostgreSQL 15+ | Catalogue, événements, agrégats, DLQ |
| Cache | Redis 7+ | Recommandations, top tracks live |
| Stockage objet | MinIO (S3-compatible) | Parquet, checkpoints Spark |
| Simulation | Python (custom) | Simulateur P2P avec mode fraude |
| Conteneurisation | Docker Compose | Stack complète locale |

---

## Progression — Les 3 phases

### Phase 1 — Data Pipelines Production (Lundi + Mardi, ~14h)

Construire le socle batch de SPOTIFY avec Airflow.

**Issues à fermer : #1 → #10**

```
#1  Setup Docker Compose
#2  Schéma PostgreSQL complet
#3  Data generator (faker)
#4  DAG catalog_ingestion_pipeline
#5  Simulateur P2P + Redis pub/sub
#6  DAG streaming_events_pipeline
#7  DAG aggregation_pipeline + MinIO
#8  DAG recommendation_pipeline
#9  DAG dlq_reprocessing_pipeline
#10 Tests pytest + README + doc_md
```

**Critères de validation Phase 1 :**
- [ ] Les 5 DAGs s'exécutent sans erreur avec le simulateur P2P actif
- [ ] Le catalogue est peuplé avec les données des 3 labels fournis
- [ ] Les agrégats sont cohérents avec les données source
- [ ] Les recommandations sont générées et accessibles dans Redis
- [ ] La DLQ capture les événements défectueux sans bloquer les pipelines
- [ ] Une suite pytest couvre structure + transformations

---

### Phase 2 — Streaming & Temps Réel (Mercredi PM + Jeudi, ~10h)

Faire évoluer la stack vers le temps réel avec Kafka et Spark.

**Issues à fermer : #11 → #20**

```
#11 Cluster Kafka KRaft dans docker-compose
#12 Migration simulateur P2P → Kafka (+ Redis maintenu)
#13 Premier job Spark : lecture topics, affichage console
#14 Job streaming_trends_job (fenêtres temporelles)
#15 Watermarking + gestion late events
#16 Exactly-once semantics bout-en-bout
#17 Job streaming_enrichment_job (jointures stream-static)
#18 Job fraud_detection_job (stateful, flatMapGroupsWithState)
#19 DAG reconciliation_pipeline (pont batch ↔ streaming)
#20 DAG late_events_reprocessing
```

**Critères de validation Phase 2 :**
- [ ] Les 3 jobs Spark tournent en continu
- [ ] Les tendances temps réel se mettent à jour en quelques secondes
- [ ] La détection de fraude génère des alertes correctes
- [ ] Après arrêt/relance Spark, reprise sans perte ni doublon
- [ ] Les agrégats batch et streaming convergent
- [ ] Les late events sont routés et retraités par Airflow

---

### Phase 3 — Interconnexion inter-groupes (Vendredi matin, ~3h)

Connecter votre instance aux instances des autres groupes.

**Issues à fermer : #21 → #25**

```
#21 Data contracts inter-groupes (formats communs)
#22 DAG catalog_federation_pipeline
#23 P2P cross-group (topics partagés)
#24 Top 50 Global SPOTIFY (agrégation cross-group)
#25 Chaos engineering + documentation finale
```

**Critères de validation Phase 3 :**
- [ ] Catalogue fédéré contient les tracks des autres groupes
- [ ] Au moins un transfert P2P cross-group fonctionne
- [ ] Le Top 50 Global agrège les données de tous les groupes
- [ ] Les données externes invalides partent en DLQ
- [ ] Un data contract documenté définit les formats inter-groupes

---

## Structure du repo

```
SPOTIFY/
├── README.md                          ← ce fichier
├── docker-compose.yml                 ← stack complète (à compléter)
├── .env.example                       ← variables d'environnement
│
├── dags/                              ← Phase 1 + ponts Phase 2
│   ├── catalog_ingestion_pipeline.py
│   ├── streaming_events_pipeline.py
│   ├── aggregation_pipeline.py
│   ├── recommendation_pipeline.py
│   ├── dlq_reprocessing_pipeline.py
│   ├── late_events_reprocessing.py    ← Phase 2
│   ├── reconciliation_pipeline.py     ← Phase 2
│   ├── catalog_federation_pipeline.py ← Phase 3
│   └── global_aggregation_pipeline.py ← Phase 3
│
├── spark_jobs/                        ← Phase 2
│   ├── streaming_trends_job.py
│   ├── streaming_enrichment_job.py
│   ├── fraud_detection_job.py
│   └── global_metrics_streaming_job.py ← Phase 3
│
├── kafka/
│   ├── topics_config.yml
│   ├── schemas/                       ← schémas Avro/JSON
│   └── cross_group_config.yml         ← Phase 3
│
├── contracts/                         ← Phase 3 (inter-groupes)
│   ├── catalog_federation_schema.json
│   ├── p2p_cross_request_schema.json
│   └── global_metrics_schema.json
│
├── src/
│   ├── p2p_simulator/                 ← simulateur principal
│   ├── transformations/               ← fonctions de transformation
│   └── data_generator/                ← génération de données faker
│
├── plugins/
│   ├── operators/                     ← operators Airflow custom
│   └── hooks/
│
├── sql/                               ← scripts SQL init
├── tests/
│   ├── unit/
│   ├── integration/
│   └── structure/                     ← tests structure DAGs
│
├── docs/
│   ├── ARCHITECTURE.md                ← votre diagramme d'archi
│   ├── DATA_MODEL.md                  ← votre modèle de données
│   └── RUNBOOK.md                     ← procédures incidents
│
└── solutions/                         ← déverrouillé vendredi soir
```

---

## Démarrage rapide

```bash
# 1. Cloner et configurer
git clone https://github.com/<votre-groupe>/spotify-m1.git
cd spotify-m1
cp .env.example .env

# 2. Lancer la stack Phase 1
docker compose up -d

# 3. Vérifier que tout est up
docker compose ps

# 4. Accéder aux UIs
# Airflow  : http://localhost:8080  (admin / admin)
# MinIO    : http://localhost:9001  (minioadmin / minioadmin)
# Kafka UI : http://localhost:8090  (Phase 2)
```

---

## Organisation Git

```
main           ← branche stable, protégée
├── feat/batch-pipelines    ← Phase 1
├── feat/kafka-streaming    ← Phase 2
└── feat/inter-group        ← Phase 3
```

**Convention des commits :**
```
feat(dag): add catalog_ingestion retry logic
fix(spark): correct watermark threshold on streaming_trends
docs(readme): update architecture diagram
test(unit): add transformation tests for enrichment
```

**Workflow :**
1. Créer une branche depuis `main`
2. Travailler, committer régulièrement
3. Ouvrir une PR avec description de ce qui est fait
4. Code review par un membre du groupe
5. Merge après validation

---

## Rôles suggérés

| Rôle | Périmètre principal |
|------|---------------------|
| Data Engineer — Batch | DAGs Airflow, ingestion catalogue, agrégation, DLQ, tests |
| Data Engineer — Streaming | Jobs Spark, topics Kafka, fenêtres temporelles, enrichissement |
| Data Engineer — Infra & P2P | Docker Compose, cluster Kafka, simulateur P2P, réseau |
| Data Engineer — Qualité | Exactly-once, watermarking, fraude, réconciliation, recovery |

> Dans un groupe de 3, fusionner Qualité/Fiabilité avec Streaming. Chaque membre doit comprendre **l'ensemble** de l'architecture.

---

## Ressources

- [Documentation Airflow 2.x](https://airflow.apache.org/docs/)
- [Kafka Quickstart](https://kafka.apache.org/quickstart)
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Questions → ouvrir une issue avec le label `question`](../../issues/new?labels=question)

---

## Grille d'évaluation

| Critère | Poids | Attendu |
|---------|-------|---------|
| Architecture & conception | 15% | Diagramme clair, choix justifiés, séparation batch/streaming cohérente |
| Pipelines batch (Airflow) | 20% | DAGs fonctionnels, robustes, idempotents, parallélisés, testés |
| Streaming (Kafka + Spark) | 20% | Topics bien conçus, jobs opérationnels, fenêtres correctes |
| Fiabilité & résilience | 15% | Exactly-once, watermarking, recovery, réconciliation |
| Interconnexion inter-groupes | 15% | Fédération catalogue, P2P cross-group, classement global |
| Qualité & documentation | 10% | Tests pytest, README, doc_md, data contracts, code propre |
| Soutenance & collaboration | 5% | Clarté, chaque membre explique l'ensemble, esprit d'équipe |

---

> **La soutenance finale est une démonstration live, pas un diaporama. Votre meilleur argument est un système qui tourne.**

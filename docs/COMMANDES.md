# Commandes — Issues 3 à 12

## Prérequis : environnement virtuel

```bash
cd /Users/christianodavid/Documents/cours/data_pipeline/spotify_data_stream
source .venv/bin/activate
pip install -r requirements.txt
```
Prépare ton poste de travail avec tous les outils nécessaires pour faire tourner la plateforme localement.

---

## Issue 3 — Data Generator

> On génère un faux catalogue musical (artistes, albums, morceaux) avec des données réalistes, puis on l'envoie dans notre stockage MinIO. C'est le point de départ de toute la plateforme : sans catalogue, il n'y a rien à écouter ni à recommander.

```bash
python -m src.data_generator.generate_catalog --artists 15 --upload
```
Simule les données que trois maisons de disques (SunSet Records, NightWave Music, Urban Pulse) nous auraient envoyées : artistes, albums, morceaux avec leurs métadonnées. Sans ce catalogue, la plateforme n'a aucun morceau à proposer aux utilisateurs.

```bash
ls data/labels/
```
Vérifie que les fichiers des trois labels ont bien été créés avant l'envoi.

```bash
pytest tests/unit/test_transformations.py::TestDataGenerator -v
```
S'assure que les données générées ont bien la forme attendue par notre pipeline d'ingestion — comme un contrôle qualité avant réception.

**✅ Critère de validation :**
- Terminal : `4 passed`
- **MinIO UI** → [http://localhost:9001](http://localhost:9001) → bucket `labels-raw` → 3 fichiers JSON des labels visibles

---

## Issue 4 — catalog_ingestion_pipeline

> On implémente le premier vrai pipeline Airflow qui lit les fichiers JSON des labels depuis MinIO, vérifie que les données sont correctes, normalise les noms d'artistes, puis charge tout en base de données. C'est le pipeline qui "peuple" la plateforme avec le catalogue musical.

```bash
docker exec spotify_data_stream-airflow-scheduler-1 airflow dags unpause catalog_ingestion_pipeline
```
Autorise le pipeline d'ingestion du catalogue à tourner — par défaut il est mis en attente pour éviter qu'il parte sans données.

```bash
docker exec spotify_data_stream-airflow-scheduler-1 airflow dags trigger catalog_ingestion_pipeline
```
Lance le chargement du catalogue en base : les morceaux, artistes et albums des trois labels sont importés dans notre base de données, validés et normalisés. C'est le socle de toute la plateforme — sans ça, on ne sait pas quelles chansons existent.

```bash
docker exec spotify_data_stream-postgres-1 psql -U spotify -d spotify \
  -c "SELECT COUNT(*) FROM tracks; SELECT COUNT(*) FROM artists;"
```
Confirme que les morceaux et artistes sont bien en base et disponibles pour le reste de la plateforme.

```bash
docker exec spotify_data_stream-airflow-scheduler-1 airflow dags trigger catalog_ingestion_pipeline
```
Relance le pipeline une deuxième fois pour prouver qu'il est robuste : relancer deux fois le même import ne crée pas de doublons dans le catalogue.

```bash
docker exec spotify_data_stream-airflow-worker-1 python -m pytest \
  /opt/airflow/tests/structure/test_dag_structure.py::TestCatalogIngestionDAG -v
```
Vérifie que le pipeline est bien construit selon les standards du projet.

**✅ Critère de validation :**
- **Airflow UI** → [http://localhost:8080](http://localhost:8080) → `catalog_ingestion_pipeline` → cercle vert
- `SELECT COUNT(*) FROM tracks` > 0, même résultat après 2e run (idempotence)
- Tests : `PASSED`

---

## Issue 5 — Simulateur P2P

> On crée un programme qui simule de vrais utilisateurs en train d'écouter de la musique sur un réseau peer-to-peer. Il génère en continu des événements d'écoute et les publie dans Redis. Sans lui, tous nos pipelines de traitement seraient vides.

```bash
python -m src.p2p_simulator.simulator --peers 10 --rate 3
```
Met en route 10 utilisateurs fictifs qui écoutent des morceaux, se connectent et se déconnectent du réseau — sans vrai trafic utilisateur, tous nos pipelines de traitement seraient vides et impossible à tester.

```bash
redis-cli -p 6380 -n 1 llen listening_events_queue
```
Vérifie combien d'événements d'écoute attendent d'être traités par nos pipelines — ce chiffre doit augmenter tant que des utilisateurs "écoutent".

```bash
redis-cli -p 6380 subscribe listening_events
```
Observe en direct les événements générés : chaque ligne représente un utilisateur en train d'écouter un morceau quelque part dans le monde.

**✅ Critère de validation :**
- Le simulateur tourne 5 minutes sans planter
- La queue se remplit progressivement
- Les événements JSON défilent en temps réel dans le subscribe

---

## Issue 6 — streaming_events_pipeline

> On implémente le pipeline qui traite les écoutes en temps quasi-réel : toutes les 5 minutes, il vide la file d'attente, valide chaque écoute, l'enrichit avec les infos du morceau, l'archive en Parquet sur MinIO et l'enregistre en base. C'est lui qui "donne de la valeur" aux données du simulateur.

```bash
python -m src.p2p_simulator.simulator --peers 10 --rate 5
```
Maintient le flux d'écoutes actif — ce pipeline en a besoin pour avoir quelque chose à traiter.

```bash
docker exec spotify_data_stream-airflow-scheduler-1 airflow dags unpause streaming_events_pipeline
docker exec spotify_data_stream-airflow-scheduler-1 airflow dags trigger streaming_events_pipeline
```
Lance le pipeline central qui transforme les écoutes brutes en données exploitables : il valide chaque écoute, l'enrichit avec les infos du morceau (titre, genre, artiste), la sauvegarde en fichier d'archive et l'enregistre en base. C'est lui qui "donne de la valeur" aux données du simulateur.

```bash
docker exec spotify_data_stream-postgres-1 psql -U spotify -d spotify \
  -c "SELECT COUNT(*) FROM listening_events;"
```
Confirme que des écoutes sont bien enregistrées — ce compteur représente le nombre de streams traités par la plateforme.

**✅ Critère de validation :**
- **Airflow UI** → `streaming_events_pipeline` → toutes les tâches vertes
- `SELECT COUNT(*) FROM listening_events` > 0
- **MinIO UI** → bucket `spotify-parquet` → archives des écoutes présentes, organisées par date et heure

---

## Issue 7 — aggregation_pipeline

> On implémente le pipeline qui calcule les statistiques quotidiennes de la plateforme : top 50 des morceaux les plus écoutés, streams et auditeurs uniques par artiste, taux de cache du réseau P2P. Ce sont les données qui alimenteraient les charts et tableaux de bord d'un vrai Spotify.

```bash
docker exec spotify_data_stream-airflow-scheduler-1 airflow dags unpause aggregation_pipeline
docker exec spotify_data_stream-airflow-scheduler-1 airflow dags trigger aggregation_pipeline
```
Lance le calcul des statistiques quotidiennes : quels sont les 50 morceaux les plus écoutés aujourd'hui ? Combien d'auditeurs uniques pour chaque artiste ? Quel pourcentage des écoutes vient du cache local ? Ce sont les données qui alimenteraient les charts et les tableaux de bord d'un vrai Spotify.

```bash
docker exec spotify_data_stream-postgres-1 psql -U spotify -d spotify \
  -c "SELECT * FROM daily_streams ORDER BY total_streams DESC LIMIT 10;"
```
Affiche le top 10 des morceaux les plus streamés — l'équivalent du classement quotidien de la plateforme.

```bash
docker exec spotify_data_stream-airflow-worker-1 python -m pytest \
  /opt/airflow/tests/structure/test_dag_structure.py::TestAggregationDAG -v
```
Vérifie que le pipeline attend bien la fin du traitement des écoutes avant de calculer les stats — pour ne pas agréger des données incomplètes.

**✅ Critère de validation :**
- **Airflow UI** → `aggregation_pipeline` → run vert
- `SELECT * FROM daily_streams LIMIT 10` retourne des morceaux avec leur nombre de streams
- Tests : `PASSED`

---

## Issue 8 — recommendation_pipeline

> On implémente le moteur de recommandation personnalisée : il analyse les 7 derniers jours d'écoutes, trouve les utilisateurs aux goûts similaires (collaborative filtering), et génère pour chacun un top-10 de morceaux qu'il n'a pas encore écoutés. Les recommandations sont stockées pour un accès instantané.

```bash
docker exec spotify_data_stream-airflow-scheduler-1 airflow dags unpause recommendation_pipeline
docker exec spotify_data_stream-airflow-scheduler-1 airflow dags trigger recommendation_pipeline
```
Lance la génération de recommandations personnalisées : pour chaque utilisateur actif des 7 derniers jours, on calcule quels morceaux ses "voisins musicaux" ont aimé et qu'il n'a pas encore écoutés. C'est l'équivalent de la section "Recommandé pour vous" de Spotify.

```bash
docker exec spotify_data_stream-postgres-1 psql -U spotify -d spotify \
  -c "SELECT DISTINCT user_id FROM listening_events LIMIT 1;"
```
Récupère l'identifiant d'un utilisateur qui a des écoutes enregistrées, pour aller vérifier ses recommandations.

```bash
redis-cli -p 6380 -n 1 get reco:<user_id>
```
Affiche les 10 morceaux recommandés pour cet utilisateur — stockés en accès ultra-rapide pour que l'application puisse les afficher instantanément sans requête SQL.

```bash
docker exec spotify_data_stream-postgres-1 psql -U spotify -d spotify \
  -c "SELECT COUNT(*) FROM recommendations;"
```
Vérifie le nombre total de recommandations générées — une ligne par paire utilisateur/morceau recommandé.

**✅ Critère de validation :**
- **Airflow UI** → `recommendation_pipeline` → run vert
- `redis-cli get reco:<user_id>` retourne une liste de morceaux
- `SELECT COUNT(*) FROM recommendations` > 0

---

## Issue 9 — dlq_reprocessing_pipeline

> On implémente le pipeline de récupération des données corrompues : les écoutes invalides rejetées par les autres pipelines sont mises de côté dans une "Dead Letter Queue", et ce pipeline tente de les corriger périodiquement. Après 3 tentatives infructueuses, l'événement est définitivement abandonné.

```bash
docker exec spotify_data_stream-postgres-1 psql -U spotify -d spotify \
  -c "INSERT INTO dead_letter_events (id, payload, error_type, original_topic) VALUES (gen_random_uuid(), '{}'::jsonb, 'test', 'listening_events');"
```
Simule une écoute corrompue qui aurait été rejetée par nos pipelines — comme si un appareil avait envoyé un événement sans indiquer quel morceau ni quel utilisateur. Ces événements problématiques sont mis de côté pour ne pas bloquer le reste du traitement.

```bash
docker exec spotify_data_stream-airflow-scheduler-1 airflow dags unpause dlq_reprocessing_pipeline
docker exec spotify_data_stream-airflow-scheduler-1 airflow dags trigger dlq_reprocessing_pipeline
```
Lance la tentative de récupération des données rejetées : le pipeline essaie de corriger ce qui peut l'être (un timestamp manquant par exemple). Si l'événement est vraiment irrécupérable (pas d'utilisateur, pas de morceau), il est abandonné après 3 tentatives.

```bash
docker exec spotify_data_stream-postgres-1 psql -U spotify -d spotify \
  -c "SELECT status, COUNT(*) FROM dead_letter_events GROUP BY status;"
```
Visualise l'état de tous les événements problématiques : combien ont pu être récupérés, combien sont encore en attente, combien ont été définitivement abandonnés.

**✅ Critère de validation :**
- **Airflow UI** → `dlq_reprocessing_pipeline` → run vert
- Après 3 triggers : le résultat SQL montre `abandoned | 1`

---

## Issue 10 — Tests & documentation

> On vérifie que toute la Phase 1 est solide : tests unitaires sur les fonctions de traitement des données, tests de structure sur tous les pipelines Airflow, et documentation de l'architecture. C'est la validation finale avant de passer à la Phase 2.

```bash
pytest tests/unit/ -v
```
Vérifie que nos fonctions de traitement des données se comportent correctement dans tous les cas : un artiste mal formaté est bien normalisé, une écoute de 2 secondes est bien détectée comme suspecte (bot), des artistes en double sont bien dédupliqués.

```bash
docker exec spotify_data_stream-airflow-worker-1 python -m pytest \
  /opt/airflow/tests/structure/ -v
```
Vérifie que tous nos pipelines Airflow sont bien construits : les bonnes étapes dans le bon ordre, les bons horaires de déclenchement, la documentation présente pour chaque pipeline.

**✅ Critère de validation :**
- `18 passed` (tests métier) + `16 passed` (tests pipeline), `0 failed`

---

## Issue 11 — Cluster Kafka

> On monte le système de messagerie temps réel avec 3 serveurs Kafka redondants en mode KRaft (sans ZooKeeper). Les 6 canaux de communication sont créés automatiquement. C'est l'infrastructure qui permettra à Spark de consommer les événements en Phase 2.

```bash
docker compose up -d kafka-1 kafka-2 kafka-3 kafka-ui kafka-init
```
Démarre le système de messagerie en temps réel avec 3 serveurs redondants : si l'un tombe, les deux autres continuent. Les 6 canaux de communication sont créés automatiquement (écoutes, événements réseau, mises à jour du catalogue, alertes fraude, etc.).

```bash
docker ps | grep kafka
docker compose logs kafka-init
```
Vérifie que les 3 serveurs sont bien démarrés et que les 6 canaux ont été créés correctement.

**✅ Critère de validation :**
- **Kafka UI** → [http://localhost:8090](http://localhost:8090) → **Topics** → 6 canaux listés avec leurs configs
- **Kafka UI** → **Brokers** → 3 serveurs actifs

---

## Issue 12 — Simulateur → Kafka

> On adapte le simulateur pour qu'il envoie chaque écoute simultanément dans deux directions : Redis (pour les pipelines Airflow existants qui continuent de tourner) et Kafka (pour les futurs jobs Spark en temps réel). Les deux phases coexistent sans se perturber.

```bash
pip install confluent-kafka==2.3.0
```
Installe le connecteur qui permet au simulateur de parler à Kafka.

```bash
docker compose stop kafka-1 && docker compose up -d kafka-1
```
Redémarre le premier serveur Kafka pour qu'il soit accessible depuis ton Mac (il est dans Docker, donc il faut un "pont" entre l'intérieur et l'extérieur).

```bash
python -m src.p2p_simulator.simulator --peers 10 --rate 5
```
Lance le simulateur amélioré qui envoie maintenant chaque écoute dans deux directions simultanément : vers Redis pour les pipelines batch existants (Phase 1), et vers Kafka pour les futurs pipelines temps réel (Phase 2) — les deux coexistent sans se perturber.

```bash
docker exec spotify_data_stream-airflow-scheduler-1 airflow dags list-runs \
  -d streaming_events_pipeline 2>&1 | tail -5
```
Confirme que l'ajout de Kafka n'a rien cassé — les pipelines de la Phase 1 continuent de tourner normalement.

**✅ Critère de validation :**
- **Kafka UI** → topic `listening_events` → onglet **Messages** → les écoutes des utilisateurs arrivent en temps réel
- **Airflow UI** → `streaming_events_pipeline` → les runs continuent à passer en vert

---

## URLs des interfaces

| Interface | URL | Login |
|-----------|-----|-------|
| Airflow   | [http://localhost:8080](http://localhost:8080) | admin / admin |
| MinIO     | [http://localhost:9001](http://localhost:9001) | minioadmin / minioadmin |
| Kafka UI  | [http://localhost:8090](http://localhost:8090) | — |

# Paperone - Sistema di Monitoraggio delle Performance del TCoE

Un sistema completo di pipeline dati e servizio API per monitorare le performance del TCoE attraverso metriche OKR (Objectives and Key Results).

## 📋 Indice dei Contenuti

- [Panoramica](#panoramica)
- [Architettura](#architettura)
- [Componenti](#componenti)
- [Metriche OKR](#metriche-okr)
- [Installazione](#installazione)
- [Utilizzo](#utilizzo)
- [Flusso dei Dati](#flusso-dei-dati)
- [Schema del Database](#schema-del-database)
- [Endpoint API](#endpoint-api)
- [Sviluppo](#sviluppo)

---

## 🎯 Panoramica

Paperone è un servizio "middleman" (intermediario) che:
- **Aggrega** dati da più fonti esterne (YouTrack, Wiki, file CSV)
- **Elabora** e analizza i dati di tracciamento delle issue e testing
- **Calcola** le metriche OKR per misurare le performance del TCoE
- **Espone** API REST per dashboard e visualizzazione via Grafana

### Caratteristiche Principali

✅ Calcolo metriche OKR in tempo reale  
✅ Aggregazione multi-source  
✅ Cache Redis per ottimizzazione performance  
✅ Persistenza PostgreSQL  
✅ Interfaccia REST con FastAPI  
✅ Tracciamento cronologico dei cambiamenti  
✅ Sincronizzazione dati automatizzata  

---

## 🏗️ Architettura

Paperone utilizza un'**architettura orientata ai servizi** deployata con Docker Compose:

```
┌─────────────────┐
│    Grafana      │  Layer di visualizzazione
└────────┬────────┘
         │ HTTP API
┌────────▼────────┐
│   Paperone      │  Elaborazione dati e API
│   (FastAPI)     │
└─┬──────┬────────┘
  │      │
  │      └────────────────┐
  │                       │
┌─▼──────┐    ┌──────────▼──┐
│ Redis  │    │ PostgreSQL   │  Layer di persistenza
└────────┘    └──────────────┘

Fonti di Dati Esterne:
├── YouTrack (Tracciamento issue)
├── Wiki (Changelog/Info release)
├── File CSV (Dati test)
└── Configurazione Manuale
```

---

## 🔧 Componenti

### 1. **Servizio API Paperone** (FastAPI)
- Server applicativo principale
- Gestisce l'elaborazione dati e i calcoli OKR
- Espone endpoint REST
- Coordina l'accesso a database e cache

### 2. **Database** (PostgreSQL)
- Memorizza issue, custom field e cronologia dei cambiamenti
- Accesso basato su ORM con SQLAlchemy
- Supporta query complesse per il calcolo delle metriche

### 3. **Layer Cache** (Redis)
- Accelera le risposte API
- Cachea le metriche OKR calcolate
- Riduce il carico del database

### 4. **Fonti Dati**
- **YouTrack**: Dati issue e attività
- **Wiki**: Informazioni release firmware
- **Mapper Hardcoded**: Per i casi in cui non è disponibile l'accesso diretto alle fonti interessate

---

## 📊 Metriche OKR

### OKR 1: Defect Rate
Misura il rapporto tra bug segnalati dai clienti e bug totali rilevati.

```
Defect Rate = Bug Clienti / Bug Totali
```

**Calcolato da:**
- Issue con Type = "Bug"
- Raggruppate per Origine (Cliente vs Interno) e Prodotto
- Periodo: Mensile

**Output include:**
- Percentuale defect rate mensile
- Contesto rilascio firmware
- Ratios breakdown per origine/prodotto

---

### OKR 2: Tempo di Esecuzione dei Test
Misura il tempo dedicato a validazioni e test relativamente alla durata totale della fase di test.

**Intervalli chiave:**
- **Release RC0**: Inizio fase di test
- **Release Produzione**: Fine fase di test
- **Durata Fase Test**: Tempo tra RC0 e produzione

**Metriche calcolate:**
- Durata totale fase test (assoluta e ore lavorative)
- Suddivisione tempo: Validazioni / Test Manuali / Test Automatizzati
- Media mobile 6 mesi per analisi trend

**Assunzioni:**
- Team lavora Lun-Ven, 8:00-17:00 UTC (1 ora pausa 12:00-13:00)
- Contano solo le ore lavorative (festivi esclusi)
- Contano solo validazioni dopo RC0

---

### OKR 3: Test per FTE
Misura la produttività di testing relativamente alla capacità del team.

```
Test per FTE = Test Totali Eseguiti / FTE (Full-Time Equivalent)
```

FTE calcolato come: Somma delle ore lavorative settimanali per membro del team

**Periodo:** Periodi di 2 settimane

---

### OKR 4: Turnaround Validazioni
Metrica comprehensiva che traccia il ciclo di vita delle validazioni e gli indicatori di qualità.

**Bucket di classificazione validazioni:**
- **Pre**: Assegnate e completate prima di RC0
- **During**: Assegnate e completate dopo RC0
- **Slipped (TCoE)**: Assegnate pre-RC0, completate post-RC0 (responsabilità TCoE)
- **Slipped (Not TCoE)**: Assegnate pre-RC0, completate post-RC0 (cause esterne)

**Metriche calcolate per firmware:**
- Tempo medio speso per validazione
- Distribuzione quote tempo (lavoro/attesa/bloccato)
- Profondità e posizione coda
- Analisi tempo bloccato
- Rilevamento over-assignment

---

## 🚀 Installazione

### Prerequisiti
- Docker & Docker Compose

### Quick Start

1. **Clonare il repository**
```bash
git clone <repository-url>
cd paperone
```

2. **Configurare l'ambiente**
```bash
cp .env.base .env
# Modificare .env con le proprie impostazioni
# Recuperare un Token di accesso valido da YouTrack e inserirlo nel file .env
```

3. **Inizializzare il database**
- Repecuperare l'sql dump di bugia e inserirlo in `db_init/bugia.sql.old`  

4. **Avviare i servizi**
```bash
docker-compose up -d
```

5. **Accedere all'applicazione**
- API: http://localhost:8000
- Grafana: http://localhost:3000
- API Docs: http://localhost:8000/docs

---

## 📈 Utilizzo

### Recupero Metriche OKR

```bash
# Ottenere Defect Rate
curl http://paperone:8000/okr1

# Ottenere Durata Fase Test
curl http://paperone:8000/okr2

# Ottenere Test per FTE
curl http://paperone:8000/okr3

# Ottenere Metriche Validazioni
curl http://paperone:8000/okr4
```

***Paperone non è visibile dall'esterno, per interrogare i suoi endpoint bisogna passare per Grafana***

### Sincronizzazione Dati

Paperone sincronizza automaticamente i dati da YouTrack:

```python
from services.issue_repository import IssueRepository

# Aggiornare issue e custom field
IssueRepository.upsert_issues(issue_data)

# Aggiornare cronologia cambiamenti
IssueRepository.upsert_activity_items(activity_data)
```

### Integrazione Grafana

Grafana interroga gli endpoint di Paperone e visualizza le metriche:


1. Aggiungere Paperone come fonte di dati
2. Creare dashboard usando query JSON
3. Configurare intervalli di aggiornamento (metriche sono cachate)

***Grafana ha 4 dashboard ottenute via provisioning, per modificarle bisogna modificare i json di provisioning in /dashboards***

---

## 🔄 Flusso dei Dati

### Flusso di Query (Calcolo OKR)

```
1. Client richiede /okr{1-4}
   ↓
2. Paperone riceve richiesta
   ↓
3. Verificare cache Redis
   ├─→ Trovato: Ritornare dati cachati
   └─→ Non trovato: Continuare
   ↓
4. Interrogare database PostgreSQL
   ↓
5. Elaborare dati grezzi:
   - Applicare logica di business
   - Calcolare metriche
   - Generare aggregazioni
   ↓
6. Memorizzare in cache Redis
   ↓
7. Ritornare al client
   ↓
8. Grafana visualizza i dati
```

***OKR2 & OKR4 vengono calcolati ogni volta chè avviene il pull e salvati in cache, non vengono ri-elaborati se non al successivo pull***

### Flusso Ingestione Dati

```
Fonti Esterne
├── API YouTrack
│   ├─→ Dati issue (id, summary, created, updated)
│   ├─→ Custom field (Type, Origine, Product, Assignee, etc.)
│   └─→ Cronologia attività (cambiamenti field con timestamp)
├── Wiki/Changelog
│   └─→ Date rilascio firmware

            ↓

Servizio Paperone
├─→ Estrarre e normalizzare dati
├─→ Applicare trasformazioni
├─→ Validare rispetto allo schema
└─→ Inserire/aggiornare database

            ↓

Database PostgreSQL
├─→ Issues
├─→ IssueCustomField
├─→ IssueCustomFieldChange
├─→ FieldValue e Value (polimorfici)
└─→ Cronologia Custom Field

            ↓

Cache Redis
└─→ Metriche OKR calcolate
```

---

## 📚 Schema del Database

### Entità Principali

#### Issue
Rappresenta un elemento di lavoro da YouTrack.

```sql
CREATE TABLE issues (
    id SERIAL PRIMARY KEY,
    youtrack_id VARCHAR UNIQUE,
    id_readable VARCHAR,
    summary TEXT,
    parent_id VARCHAR,
    created TIMESTAMP,
    updated TIMESTAMP,
    tags TEXT[]
);
```

#### IssueCustomField
Memorizza i valori dei custom field per le issue.

```sql
CREATE TABLE issue_custom_fields (
    id SERIAL PRIMARY KEY,
    name VARCHAR,
    issue_id VARCHAR,
    value_id UUID,
    UNIQUE(name, issue_id),
    FOREIGN KEY (value_id) REFERENCES field_values(id)
);
```

#### IssueCustomFieldChange
Traccia i cambiamenti cronologici ai custom field.

```sql
CREATE TABLE issue_custom_field_changes (
    id SERIAL PRIMARY KEY,
    field_id INTEGER,
    old_value_id UUID,
    new_value_id UUID,
    timestamp TIMESTAMP,
    FOREIGN KEY (field_id) REFERENCES issue_custom_fields(id)
);
```

#### FieldValue e Value (Polimorfi)
Memorizzazione generico di valori con tabelle figlie specifiche per tipo.

```sql
CREATE TABLE field_values (
    id UUID PRIMARY KEY
);

CREATE TABLE values (
    id UUID PRIMARY KEY,
    type VARCHAR,
    FOREIGN KEY (id) REFERENCES field_values(id)
);

-- Tabelle figlie:
CREATE TABLE string_values (
    id UUID PRIMARY KEY,
    value VARCHAR,
    FOREIGN KEY (id) REFERENCES values(id)
);

CREATE TABLE number_values (
    id UUID PRIMARY KEY,
    value NUMERIC,
    FOREIGN KEY (id) REFERENCES values(id)
);

CREATE TABLE date_values (
    id UUID PRIMARY KEY,
    value TIMESTAMP,
    FOREIGN KEY (id) REFERENCES values(id)
);

CREATE TABLE time_values (
    id UUID PRIMARY KEY,
    value INTERVAL,
    FOREIGN KEY (id) REFERENCES values(id)
);
```

### Relazioni

```
Issue
  ├─ 1:N → IssueCustomField
  │         ├─ 1:1 → FieldValue → Value (polimorfici)
  │         └─ 1:N → IssueCustomFieldChange
  │                   ├─ 1:1 → FieldValue (valore vecchio)
  │                   └─ 1:1 → FieldValue (valore nuovo)
  └─ N:1 → Issue (parent_id)
```

---

## 🔌 Endpoint API

### GET /okr1
**Metriche Defect Rate**

Response:
```json
[
  {
    "date": "2026-05-01T00:00:00",
    "Defect Rate": 0.25,
    "FW Released": 1,
    "Cliente-Product1-ratio": 0.15,
    "Internal-Product1-ratio": 0.10
  }
]
```

### GET /okr2
**Metriche Durata Fase Test**

Response:
```json
[
  {
    "fw": "1.2.3",
    "start": 1715000000,
    "test_phase_duration": 86400,
    "validations_time_share": 30.5,
    "manual_time_share": 25.3,
    "automated_time_share": 35.2,
    "other": 9.0,
    "media_a_6_mesi": 90000
  }
]
```

### GET /okr3
**Test per FTE**

Response:
```json
{
  "period": "2026-05-15",
  "total_tests": 450,
  "fte": 2.5,
  "tests_per_fte": 180
}
```

### GET /okr4
**Metriche Turnaround Validazioni**

Response:
```json
[
  {
    "fw": "1.2.3",
    "date": "2026-05-01T00:00:00",
    "avg_time_spent": 3600,
    "time_spent_share": 0.45,
    "blocked_share": 0.15,
    "waiting_share": 0.40,
    "queue": 1.2,
    "pre": 0.25,
    "during": 0.50,
    "blocked": 0.10,
    "overassigned": 0.05,
    "count": 20
  }
]
```

---

## 💻 Sviluppo

### Struttura del Progetto

```
paperone/
├── app.py                          # Entry point applicazione FastAPI
├── requirements.txt                # Dipendenze Python
├── Dockerfile                      # Configurazione container
│
├── models/                         # Modelli ORM SQLAlchemy
│   ├── base.py                    # Classe modello base
│   ├── issues.py                  # Issue, IssueCustomField, IssueCustomFieldChange
│   ├── value.py                   # FieldValue, Value, e sottoclassi
│   └── users.py                   # Modelli User
│
├── services/                       # Logica di business e accesso dati
│   ├── postgres_engine.py         # Connessione database e session factory
│   ├── redis_client.py            # Operazioni cache Redis
│   ├── logger.py                  # Configurazione logging
│   ├── issue_repository.py        # Accesso dati issue e calcolo OKR1/2/4
│   ├── product_repository.py      # Dati prodotto/firmware e release
│   └── test_repository.py         # Dati test e calcolo OKR3
│
├── youtrack/                       # Integrazione YouTrack
│   ├── __init__.py
│   └── youTrack.py                # Sincronizzazione background con API YouTrack
│
└── dashboards/                     # Definizioni dashboard Grafana
```

### File Chiave

#### `issue_repository.py`
Logica principale di calcolo OKR:
- **`okr1()`**: Analisi defect rate bug
- **`okr2()`**: Durata fase test e suddivisione effort team
- **`okr4()`**: Turnaround validazioni e metriche qualità
- **`upsert_issues()`**: Sincronizzazione issue da YouTrack
- **`upsert_activity_items()`**: Tracciamento cronologico cambiamenti field

#### `product_repository.py`
Informazioni firmware e release:
- Date release RC0 (attualmente hardcoded)
- Date release produzione (da Wiki)
- Mapping prodotto-firmware

#### `youtrack.py`
Servizio background per sincronizzazione dati continua:
- Poll API YouTrack a intervalli regolari
- Fetch issue nuove e cambiamenti attività
- Memorizzazione in PostgreSQL

### Aggiungere Nuove Metriche OKR

1. Creare metodo di calcolo nel repository appropriato:
```python
@staticmethod
def okrN():
    """Descrizione OKR"""
    # Implementazione
    return result_list
```

2. Aggiungere endpoint API in `app.py`:
```python
@app.get('/okrN')
def OKRN():
    return IssueRepository.okrN()
```

3. Aggiornare dashboard Grafana per visualizzare la nuova metrica


---

## 🔐 Note di Sicurezza

⚠️ **Stato Attuale:** Solo sicurezza di base

- Grafana fornisce layer di autenticazione
- Nessuna autenticazione API su endpoint Paperone
- Credenziali database in variabili d'ambiente
- Considerare implementazione di:
  - Autenticazione token API
  - Rate limiting
  - Validazione input
  - HTTPS/TLS
  - Crittografia database

---

## 📝 Assunzioni e Limitazioni

### Calcolo Ore Lavorative
- Solo Lun-Ven (weekend esclusi)
- Festivi italiani (provincia Pisa) esclusi
- Ore di lavoro 8:00-17:00 CET
- 1 ora pausa pranzo (12:00-13:00)

### Assunzioni OKR4 Validation (Assunzioni usate per calcolare il il numero di validation massimo che il gruppo sarebbe stato in grado di eseguire)
- Una validation per membro TCoE alla volta
- Lavoro continuo fino al completamento
- Data creazione usata se nessun assignment TCoE trovato
- Coda calcolata dall'ordinamento timestamp completamento

### Limitazioni Dati
- Date RC0 attualmente hardcoded (non da API)
- Dati BugIA importati manualmente da CSV
- Nessuna integrazione diretta License Server
- Parsing changelog Wiki richiesto

---


## 📞 Supporto & Troubleshooting

### Problemi Comuni

**Metriche non si aggiornano:**
- Verificare connessione Redis: `redis-cli PING`
- Verificare che PostgreSQL è in esecuzione
- Controllare log servizio background YouTrack
- Pulire cache manualmente se necessario

**Dati mancanti nelle dashboard:**
- Controllare mapping firmware/prodotto in `product_repository.py`
- Assicurare credenziali API YouTrack valide
- Rivedere log database

**Problemi di performance:**
- Controllare cache hit rate Redis
- Monitorare tempo query database
- Considerare paginazione per large dataset
- Scalare orizzontalmente se necessario

---


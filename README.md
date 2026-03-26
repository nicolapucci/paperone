# Documentazione del progetto  

## Introduzione

Lo scopo del progetto è monitorare l’andamento del TCoE nelle diverse fasi di lavorazione.

L’architettura si basa sull’utilizzo di un componente intermedio (“middleman”) che si occupa di:
- aggiornare i dati  
- elaborare le informazioni provenienti da fonti esterne  

Le principali fonti includono:
- server di servizi utilizzati dal team  
- file statici  
- wiki  

I dati elaborati vengono poi utilizzati da Grafana per la generazione delle dashboard.

---

## Servizi

Il progetto è stato sviluppato utilizzando Docker e comprende quattro servizi (container) principali:

- Redis  
  → sistema di caching utilizzato per velocizzare le risposte  

- PostgreSQL  
  → database relazionale che ospita i dati del progetto  

- Paperone  
  → server sviluppato in FastAPI (Python)  
  → si occupa di:
    - interrogare le fonti esterne  
    - leggere/scrivere dal database  
    - elaborare i dati  
    - fornire i dati a Grafana tramite API  

- Grafana  
  → piattaforma di visualizzazione  
  → utilizza i dati forniti da Paperone per costruire le dashboard  

---

## Sicurezza

Attualmente il progetto non prevede un sistema di sicurezza strutturato.

- L’unico livello di protezione presente è quello offerto da Grafana  
## Il concetto di OKR

Un OKR (Objective and Key Results) è un indicatore utilizzato per valutare le performance rispetto a specifici obiettivi.

Nel nostro caso, gli OKR definiti sono i seguenti:

- OKR 1: Defect-rate  
- OKR 2: Tempo di esecuzione dei test  
- OKR 3: Test per FTE  
- OKR 4: Validation  

---

## OKR 1: Defect-rate

Questo OKR misura il defect-rate, ovvero la percentuale che rappresenta il rapporto tra:

- bug segnalati dai clienti  
- totale dei bug rilevati dall’azienda  

---

## OKR 2: Tempo di esecuzione dei test

Questo OKR misura il tempo di esecuzione della fase di test, suddiviso per attività specifiche all’interno di un intervallo temporale definito.

Definizione dell’intervallo:
- Data di inizio  
  → dedotta dalla pubblicazione dell’RC0  

- Data di fine  
  → approssimata dalla pubblicazione del changelog  

---

### Assunzioni

- Un membro del team non esegue test se ha validation assegnati e in lavorazione  
- I validation sono suddivisi in sessioni distinte  

---

### Regole di calcolo

- Vengono considerate solo le sessioni di validation successive alla pubblicazione dell’RC0  
- Se una sessione attraversa il momento di pubblicazione dell’RC0:  
  → viene conteggiato solo il tempo compreso tra RC0 e la fine della lavorazione  

---

## OKR 3: Test per FTE

Questo OKR misura il rapporto tra:

- numero di test eseguiti  
- FTE (Full-Time Equivalent)  

L’FTE rappresenta l’unità di misura del lavoro ed è calcolato come:

- somma delle ore lavorative settimanali dei membri del team  

Nel contesto di questo progetto:
- viene utilizzata come unità di riferimento un periodo di 2 settimane lavorative  

## OKR 4: Validation

Questo OKR misura diversi aspetti legati al processo di lavorazione dei validation.

---

### Premesse

A causa della mancanza di dati fondamentali per stimare con precisione il ciclo di vita dei validation, sono state introdotte alcune assunzioni per ricavare in modo approssimativo le informazioni mancanti:

- Un membro del TCoE lavora su un solo validation alla volta  
- Ogni validation è gestito da una sola persona alla volta  
- Una volta iniziata una sessione di lavorazione, il membro continua fino alla conclusione della sessione  
  → la sessione termina quando lo stage viene impostato a `Blocked` o `Done`  
- Un membro del team non esegue test se ha validation ancora assegnati da lavorare  
- Tutti i membri lavorano a tempo pieno:  
  → dalle 09:00 CET alle 18:00 CET  
  → dal lunedì al venerdì (festivi esclusi)  

---

### Logiche di calcolo

In assenza di dati diretti, le fasi di vita dei validation vengono stimate a partire da eventi osservabili.

#### Eventi principali

Per ogni validation vengono identificati due eventi chiave:

- Assegnazione  
  → cambio del campo `assignee`  

- Fine lavorazione  
  → quando lo stage viene impostato a `Done` o `Blocked`  

Per ogni evento di fine lavorazione viene associata l'assegnazione più recente precedente.

---

#### Derivazione degli intervalli

A partire dalle coppie (assegnazione, fine lavorazione) si ricavano:

- Inizio lavorazione  
  → è la data più vicina tra:  
    - il completamento del validation precedente  
    - l'assegnazione del validation corrente  

- Code (queue)  
  → insieme dei completamenti di altre sessioni di lavorazione dello stesso assignee  
    compresi tra assegnazione e inizio lavorazione  

- Prima assegnazione al TCoE  
  → assegnazione meno recente  

- Ultimo completamento  
  → fine lavorazione più recente  

---

#### Intervalli temporali

Da questi eventi vengono definiti i seguenti intervalli:

- Lavorazione  
  → da inizio lavorazione a fine lavorazione  

- Idle  
  → da assegnazione a inizio lavorazione  

- Non in mano al TCoE  
  → tutto il tempo restante  

---

### Classificazione (bucket) dei validation

I validation vengono suddivisi in quattro categorie principali in base alla fix version e al momento della release di RC0:

- Pre  
  → validation assegnati e completati prima della release di RC0  

- During  
  → validation assegnati e completati dopo la release di RC0  

- Slipped_to_TCoE  
  → validation assegnati prima di RC0 ma completati dopo  
    per cause imputabili al TCoE  

- Slipped_not_to_TCoE  
  → validation assegnati prima di RC0 ma completati dopo  
    per cause non imputabili al TCoE  
    (es. bug scoperti in fase di pre-release e risolti successivamente)  

## Workflows

Il progetto è quasi completamente automatizzato. Tuttavia, la raccolta dei dati da BugIA e dal License Server non è ancora automatizzabile, pertanto alcuni dati devono essere inseriti manualmente.

Il progetto prevede due workflow principali:
- Raccolta dati  
- Generazione dashboard  

---

### Raccolta dati

Il prelievo dei dati avviene da diverse fonti:

- YouTrack  
  → da cui vengono recuperati i dati delle issue e i relativi Custom Fields  

- Cartella ./bugia_csv  
  → contiene i file CSV da cui vengono estratti i dati dei test  

- Wiki dei changelog delle firmware versions  
  → da cui vengono recuperate le informazioni sulle pubblicazioni dei changelog per ciascuna versione firmware  

- Mapper custom  
  → definito manualmente nel file:  
    ./services/product_repository.py  

Nota:
Non essendo disponibile un accesso diretto a BugIA e al License Server:
- i dati dei test vengono importati dai CSV presenti in ./bugia_csv  
- le pubblicazioni delle release candidate (RC) sono attualmente hardcoded  

---

### Generazione dashboard

Flusso di generazione:

1. Grafana interroga un endpoint di Paperone  
2. Paperone verifica la presenza dei dati in cache (Redis)  
   → se presenti, salta direttamente al punto 5  
3. Se i dati non sono in cache, Paperone interroga il database per ottenere i dati grezzi  
4. Paperone elabora i dati grezzi e li salva in cache  
5. Paperone restituisce i dati elaborati  
6. Grafana utilizza i dati per costruire la dashboard  

---

### Database

L’interazione con il database è gestita tramite l’ORM SQLAlchemy.

Struttura:
- Modelli  
  → definiti nella cartella:  
    ./models  

- Repository (accesso ai dati)  
  → definiti nella cartella:  
    ./services/{*}_repository.py  

Dove { * } può assumere i seguenti valori:
- issue  
- product  
- test  

I repository contengono i metodi per:
- inserimento dei dati  
- modifica dei dati  
- lettura dei dati  


## Struttura delle relazioni

### Issue
- Relazione con IssueCustomField  
  issue.id_readable = issueCustomField.issue_id  
  → Serve per recuperare i custom field associati a una issue

---

### IssueCustomField 
- Relazione con FieldValue  
  issueCustomField.value_id = field_value.id  
  → Permette di ottenere il valore corrente del custom field

---

### IssueCustomFieldChange
- Relazione con IssueCustomField  
  issueCustomFieldChange.field_id = issueCustomField.id  
  → Identifica quale custom field è stato modificato

- Relazione con FieldValue (valore precedente)  
  issueCustomFieldChange.old_value_id = field_value.id  
  → Valori rimossi nella transazione

- Relazione con FieldValue (valore nuovo)  
  issueCustomFieldChange.new_value_id = field_value.id  
  → Valori aggiunti nella transazione

---

### FieldValue
- Rappresenta un valore generico di un campo  
- Relazione con Value (tabella polimorfica)  
  field_value.id = value.id  
  → Il valore reale si ottiene tramite le tabelle figlie di value

---

### Value (tabella polimorfica)
- Contiene un campo "type" per distinguere il tipo di dato  
- Relazione:  
  value.id = <tabella_figlia>.id  

#### Tabelle figlie:
- date_values   → valori di tipo data  
- number_value  → valori numerici  
- string_value  → stringhe  
- time_value    → valori temporali  

---

## Riassunto veloce
Issue → IssueCustomField → FieldValue → Value → Tabelle figlie  
IssueCustomFieldChange traccia le modifiche (old_value / new_value)
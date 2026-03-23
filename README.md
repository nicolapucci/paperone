# Descrizione del progetto
Lo scopo del progetto è monitorare l'andamento del TCoE nelle diverse fasi di lavorazione.
Il Progetto si basa sull'uso di Grafana per la generazione di Dashboards e di un 'middleman' che gestisce aggiornamento e elaborazione dei dati provenienti da fonti esterne (Server di servizi usati dal team, file statici, wiki etc.).

### Servizi
Il progetto è stato sviluppato utilizzando Docker e comprende quattro servizi principali:
 - **Redis**   
 - **Postgresql**  
 - **Paperone**(Fast API, py)che interroga le fonti e il database per poi elaborarne i dati prima di fornirli a Grafana
 - **Grafana**

  
  >Non è stata implementata alcuna forma di sicurezza, il progetto espone solo il servizio di Grafana che ha i suoi livelli di sicurezza ed è pensato per girare all'interno di rete protetta.

### Il concetto di OKR
Un OKR, acronimo di **Objective and Key Results**, è un indicatore di valutazione.  
Nel nostro caso, gli OKR definiti sono i seguenti:

- **OKR 1**: *Defect-rate*  
- **OKR 2**: *Tempo di esecuzione dei test*  
- **OKR 3**: *Test per FTE*  
- **OKR 4**: *Validation*  

### OKR 1: Defect-rate
Il primo OKR misura il **defect-rate**,ovvero la percentuale che rappresenta il rapporto tra i bug segnalati dai clienti e il totale dei bug rilevati dall'azienda.  

### OKR 2: Tempo di esecuzione dei test
Il secondo OKR calcola il tempo di esecuzione della fase di test, suddiviso per attività specifiche. I dettagli sono i seguenti:

- La data di inizio della fase di test è stata approssimata a partire dalla data di pubblicazione dell'**RC0**.
- La data di fine è stata approssimata dalla data del **changelog**.

Per quanto riguarda la gestione dei test e delle validation (discussione che affronteremo nell'OKR 4), presupponiamo che il team faccia test quando non ha validation assegnati, o quando ce li ha ma non sono ancora sotto la sua responsabilità. Al contrario, eseguiranno validation solo quando sono loro assegnati e ci stanno effettivamente lavorando. A differenza dell'OKR 4, i **validation** sono separati in sessioni distinte, e vengono conteggiati solo quelli successivi all'**RC0**. Le distinzioni negli *slipped* (ritardi) non vengono fatte se questi sono imputabili o meno al **TCoE**.

### OKR 3: Test per FTE  
Il terzo OKR misura il rapporto tra il numero di test effettuati e l'**FTE** (Full-Time Equivalent). L'**FTE**  è un indicatore che rappresenta l'unità di misura del lavoro, ed è calcolato come la somma delle ore lavorative settimanali dei membri del team.
Noi usiamo come misura l'FTE relativo a 2 settimane di lavoro.  

### OKR 4: Validation
Quest'ultimo OKR misura vari aspetti legati alla lavorazione dei **validation** , come il numero, i tempi di esecuzione e le attese.

#### Premesse 
Poiché tutte le ipotesi sono state fatte sulla base di dati limitati, ci potrebbero essere inesattezze e margini d'errore. Tuttavia questa è l'unica metodologia che siamo riusciti a implementare data la mancanza di un approccio più affidabile.

- Un membro del **TCoE** può lavorare su un solo **validation** alla volta.
- Ogni **validation** è assegnato a una sola persona.
- Un membro del team che inizia a lavorare su un **validation** non fa altro fino a quando non lo completa.
- I **validation** hanno priorità sui **test**.
- Tutti i membri del team lavorano a tempo pieno, con orari dalle **9:00 CET alle 18:00 CET**.

**Nota**: Ogni assunzione sopra riportata comporta una perdita di affidabilità nei dati finali.

#### Logiche

Abbiamo cercato di stimare le fasi di vita dei **validation** utilizzando le assunzioni precedenti, in assenza di dati diretti.

La vita di un **validation** è divisa in fasi, definite come

- **Idle**: il **validation** è sotto la responsabilità del **TCoE**, ma non è in lavorazione.
- **Lavorazione**: il **validation** è in fase di lavorazione.
- **Non in mano al TCoE**: il **validation** non è sotto la gestione del **TCoE**.

Ogni "pezzo" può verificarsi più volte durante la vita di un **validation**. Per determinare i "pezzi" in lavorazione, ci basiamo sui cambi di stato dello **Stage**. Quando un **validation** viene messo in stato `Blocked` o `Done`, si chiude un "pezzo" di lavorazione. Successivamente, per capire chi è stato l'ultimo membro del **TCoE** a lavorare su di esso, guardiamo l'ultimo **Assignee** assegnato.

La logica per determinare l'inizio e la fine di ogni "pezzo" è la seguente:

- **Inizio**: il momento più recente tra la data di assegnazione dell'**Assignee** e il completamento della lavorazione precedente dello stesso **Assignee**.
- **Fine**: il momento in cui lo stato del **validation** cambia a `Done` o `Blocked`.

Le categorie sono quindi:

- **Lavorazione**: dall'assegnazione dell'**Assignee** fino al cambiamento di stato.
- **Idle**: dal momento in cui l'**Assignee** termina la sua lavorazione fino alla nuova assegnazione.
- **Non in mano al TCoE**: se il **validation** non è sotto la responsabilità del team, lo ignoriamo.

#### I bucket dei **validation**

I **validation** vengono suddivisi in 4 categorie principali:

- **Pre**: contiene tutti i **validation** assegnati e completati prima della release dell'**RC0**.
- **During**: contiene tutti i **validation** assegnati e completati dopo la release dell'**RC0**.
- **Slipped_to_TCoE**: contiene i **validation** assegnati prima del firmware, ma completati durante il periodo **During**.
- **Slipped_not_to_TCoE**: contiene i **validation** assegnati prima del firmware, ma completati durante il periodo **During** per motivi non imputabili al **TCoE** (ad esempio, un bug trovato in fase di pre-release e risolto con l'**RC0**).

#### Dettagli aggiuntivi:

- **Data di assegnazione al TCoE**: la prima volta in cui una **issue** viene assegnata a un membro del **TCoE**.
- **Data di completamento**: l'ultima volta in cui il **validation** è stato messo in stato "Done".
- **Le code**: rappresentano il numero di sessioni di lavorazione attese, ovvero le "completion" dello stesso **Assignee** tra la data di assegnazione e quella di completamento.


### Workflows
Il progetto è quasi completamente automatizzato. Tuttavia, la raccolta dei dati da BugIA e dal License Server non è ancora automatizzabile, pertanto i dati devono essere inseriti manualmente.

Il Progetto ha 2 workflows principali:
- **raccolta dati**   
- **generazione dashboard**

**Raccolta dati**  
Il prelievo dei dati è stato fatto da diverse fonti le quali sono:  
- YouTrack da dove abbiamo prelevato i dati delle issue con i relativi CustomFields;  
- il folder bugia_csv da cui abbiamo prelevato i dati dei test;  
- la wiki dei changelog delle fw versions da dove abbiamo prelevato le pubblicazioni dei changelog per ciascuna fw version e  
- un mapper scritto a mano contenuto in ./services/product_repository.py.

P.S.: perchè non disponevamo di un accesso diretto a BugIA e al License Server, per inserire i dati dei test abbiamo dovuto inserire il file csv nella cartella **./bugia_csv** così che, all'avvio, il progetto li elaborava e li inseriva nel db. Il mapper scritto manualmente mappa ogni versione alla data di rilascio della sua RC0 sul License Server. Ogni nuova versione, però, deve essere aggiunta manualmente al file.

**Generazione dashboard**
1. Grafana interroga un endpoint di Paperone
2. Paperone fa un controllo veloce in cache, se restituisce qualcosa passa al punto 5 
3. Paperone interroga il database per ottenere i dati grezzi
4. Paperone elabora i dati grezzi e li salva in cache
5. Paperone restituisce i dati elaborati
6. Grafana usa i dati per creare una dashboard

---

#### Database  
La relazione con il database è gestita tramite l'ORM SQLAlchemy. I modelli sono definiti nella sottocartella **./models**, mentre i metodi di inserimento, modifica e lettura si trovano nella cartella **./services/{d+*}_repository.py**, dove **{d+*}** può essere **issue**, **product** o **test**.  
  
> Lo scopo del database è quello di ridurre le richieste alle fonti (YouTrack) e abbassare i tempi necessari per fornire i dati a Grafana. 

**Struttura database**  
Il database PostgreSQL è composto da tabelle collegate tra loro mediante relazioni. Le tabelle sono:  
- **Issue** che contiene i campi id ,youtrack_id, id_readable, summary, custom_fields, parent_id, author, created e updated.  
- **IssueCustomField** che contiene i campi id, name, issue_id, issue, value_id, value e changes.  
- **IssueCustomFieldChange** che contiene id, field_id, field, old_value_id, old_value, new_value_id, new_value, timestamp e author.  
- **Product**...
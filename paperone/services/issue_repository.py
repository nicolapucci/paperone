from sqlalchemy import (
    select,
    exists,
    and_ ,
    or_,
    func,
    case,
    desc,
    Integer,
    literal_column,
    text
)
from sqlalchemy.orm import Session, aliased

from sqlalchemy.sql import over
from sqlalchemy.sql import func as sqlfunc

from sqlalchemy.dialects.postgresql import insert

import re
import pytz

from datetime import (
    datetime,
    timezone,
    timedelta
)

from services.logger import logger
from services.postgres_engine import engine
from services.product_repository import ProductRepository

from models.issues import (
    Issue,
    IssueCustomField,
    IssueCustomFieldChange
)   
from models.value import (
    DateValue,
    TimeValue,
    NumberValue,
    StringValue,
    FieldValue,
    Value
)

from services.redis_client import(
    set_okr2_data,
    get_okr2_data,
    set_okr4_data,
    get_okr4_data,
    set_custom_field_id_mapper,
    get_custom_field_id_mapper
)
import uuid
from collections import defaultdict
import bisect

"""
    issue_repository è una classe dichiarata che contiene due grandi funzionalità:
    -inserimento e aggiornamento dati;
    -analisi dei bug;
    -statistiche di validazione e OKR e
    -gestione cache.
"""

utc = pytz.UTC

BATCH_SIZE = 1000

TCoE_MEMBERS = ['Sara Tinghi', 'Simona rossi', 'Giuseppe Fragalà', 'Tommaso Capiferri', 'Nicola Montagnani']

ITERVALLO_MEDIA_MOBILE = timedelta(days= (6*30))#6 mesi

WORKING_SESSION_TRESHOLD = 5 #minimunm number of working session in a bucket to find it reliable

#definizione della funzione che calcola quanto tempo lavorativo passa tra due date
def working_hours_only_timedelta(end_date:datetime,start_date:datetime):
    end_date = end_date.replace(tzinfo=utc)
    start_date = start_date.replace(tzinfo=utc)

    current_date = start_date

    working_time = timedelta(0)

    while current_date < end_date:
        if current_date.weekday() < 5:

            working_day_start = datetime(current_date.year,current_date.month,current_date.day,8,0,0).replace(tzinfo=utc)
            working_day_end = datetime(current_date.year,current_date.month,current_date.day,17,0,0).replace(tzinfo=utc)

            start = max(current_date, working_day_start)
            end = min(end_date, working_day_end)

            if start < end:
                working_time += (end - start)

            current_date = datetime(current_date.year , current_date.month , current_date.day ,8,0,0).replace(tzinfo=utc) + timedelta(days=1)
        else:
            current_date = datetime(current_date.year , current_date.month , current_date.day ,17,0,0).replace(tzinfo=utc) + timedelta(days = 7-current_date.weekday())

    return working_time


def convert_to_timestamp(date):
    return datetime.fromtimestamp(date/1000,tz=timezone.utc)

#imposta la timezone di una data in UTC
def convert_to_timezone_aware(date):
    return date.replace(tzinfo=utc)

#estrae il nome del custom field da ActivityItems/TargetMember
def extract_field_name(targetMember:str):

    match = re.search(r'__CUSTOM_FIELD__(\w+(?: \w+)*)_\d+', targetMember)

    if match:
        field_name = match.group(1)
        return field_name
    else:
        return None

#definizione della funzione che serve a creare un oggetto della classe value scelto in 
#base al tipo dell'item passato 
def get_value_obj(item, uuid,field_name:None):
    # Se item è un dizionario, prendi il valore associato a 'name', altrimenti usa item stesso
    
    misbehaving_targets = ['Estimation','Time Left','Spent time']
    if field_name in misbehaving_targets:#tmp fix because ActivityItems only return the number and not the possible_keys
        item = {'minutes':item}

    value_possible_keys = ["name","text","fullName","minutes"]
    
    item_value = None

    if isinstance(item,dict):
        for possible_key in value_possible_keys:
            if possible_key in item.keys():
                if possible_key == 'minutes':
                    item_value = timedelta(minutes=item.get(possible_key))
                else:
                    item_value = item.get(possible_key)
    else:
        item_value = item

    # Se l'item è una stringa, crea un oggetto StringValue
    if isinstance(item_value, str):
        return StringValue(value=item_value, field_id=uuid)

    # Se l'item è un intero, crea un oggetto NumberValue
    elif isinstance(item_value, int):
        return NumberValue(value=item_value, field_id=uuid)

    # Se l'item è un datetime, crea un oggetto DateValue
    elif isinstance(item_value, datetime):
        return DateValue(value=item_value, field_id=uuid)

    elif isinstance(item_value, timedelta):
        return TimeValue(value=item_value, field_id=uuid)

    else:
        # Se nessuna delle condizioni precedenti è soddisfatta, ritorna None
        logger.warning(f"unable to classify {item} returning None")
        raise


#crea un dizionario per ottenere gli id di custom field partendo da CustomField.name e 
#Issue.id_readable
def load_custom_field_mapper():

    mapper = get_custom_field_id_mapper()
    if mapper:
        return mapper

    stmt = (
        select(IssueCustomField.id, IssueCustomField.name, Issue.id_readable)
        .join(Issue, Issue.id_readable == IssueCustomField.issue_id)
    )
    try:
        with Session(engine) as session:
            rows = session.execute(stmt).all()
    except Exception as e:
        logger.error(f"Error loading custom_field_mapper: {e}")
        raise

    mapper = {
        f"{row.name}/{row.id_readable}": row.id
        for row in rows
    }

    set_custom_field_id_mapper(mapper)

    return mapper


class IssueRepository:
    
    #metodo che restituisce il valore massimo nella colonna updated della tabella issue e che crea
    #una connessione temporanea al database locale
    @staticmethod
    def get_max_updated_issue():
        stmt = select(func.max(Issue.updated))

        with Session(engine) as session:
            max_updated = session.execute(stmt).scalar_one_or_none()
        return max_updated.strftime('%Y-%m') if max_updated else None

    # CHECK WHY SOME CUSTOM FIELDS ARE LOST (time_left, spent_time , estimation // prob due to the type of the field)
    #questo metodo che segue è uno dei due coinvolti per la prima funzionalità della classe cui fa 
    #parte
    #il metodo ha il compito di sincronizzare i dati delle issue verso il database locale
    #se l'issue esiste già viene aggiornata altrimenti viene creata
    @staticmethod
    def upsert_issues(issue_data:list):


        len_data = len(issue_data) if issue_data else 0
        logger.info(f"Received {len_data} issues")   

        if issue_data:        

            issue_rows = []
            custom_field_rows = []
            field_value_rows = []
            value_rows = []

            for data in issue_data:
                created=convert_to_timestamp(data.get('created'))
                updated=convert_to_timestamp(data.get('updated'))
                parent = data.get('parent')
                parent_issues= parent.get('issues',None) if parent else None
                id_readable = data.get('idReadable',None)


                parent_issue_id = None
                if parent_issues:
                    if isinstance(parent_issues, list) and parent_issues:
                        parent_issue_id = parent_issues[0].get('idReadable', None)#we keep only the first parent if there are more than one(temporary adjustment)
                    elif isinstance(parent_issues, dict):
                        parent_issue_id = parent_issues.get('idReadable', None)
                    
                issue_rows.append({
                "youtrack_id":data.get('id',None),
                "id_readable":id_readable,
                "summary":data.get('summary'),
                "parent_id":parent_issue_id,
                "created":created,
                "updated":updated
                })
                
                if id_readable:
                    for field in data.get('customFields',[]):
                        name = field.get('name')

                        value = field.get('value')

                        new_uuid = uuid.uuid4()

                        field_value_rows.append({
                            "id":new_uuid
                        })

                        custom_field_rows.append({
                            "name":name,
                            "value_id":new_uuid,
                            "issue_id":id_readable
                        })

                        if value is not None:       
                            if isinstance(value,list):
                                for item in value:
                                    try:
                                        value_item = get_value_obj(item,new_uuid)
                                        value_rows.append(value_item)
                                    except Exception as e:
                                        logger.warning(f"unable to create Value item: {value} / {name} / {id_readable}")    
                            else:
                                try:
                                    value_item = get_value_obj(value,new_uuid)
                                    value_rows.append(value_item)
                                except Exception as e:
                                    logger.warning(f"unable to create Value item: {value} / {name} / {id_readable}")


            upsert_issues_stmt = (
                insert(Issue)
                .values(issue_rows
                ).on_conflict_do_update(
                    index_elements=["youtrack_id"],
                    set_={
                        "summary": insert(Issue).excluded.summary,
                        "updated": insert(Issue).excluded.updated,
                        "parent_id": insert(Issue).excluded.parent_id,
                    }
                ).returning('*') 
            )

            upsert_custom_fields_stmt = (
                insert(IssueCustomField)
                .values(custom_field_rows
                ).on_conflict_do_update(
                    index_elements=["name","issue_id"],
                    set_={
                        "value_id":insert(IssueCustomField).excluded.value_id
                    }
                ).returning('*') 
            )

            insert_field_values_stmt = (
                insert(FieldValue)
                .values(field_value_rows)
                .returning('*') 
            )

            with Session(engine) as session:
                try:

                    session.execute(insert(FieldValue).values(field_value_rows))

                    session.add_all(value_rows)    

                    issue_inserted = session.execute(upsert_issues_stmt).fetchall()
                    logger.info(f"Added {len(issue_inserted)} Issues")

                    custom_field_inserted = session.execute(upsert_custom_fields_stmt).fetchall()

                    logger.info(f"Added {len(custom_field_inserted)} Custom Fields")
                    session.commit() 


                except Exception as e:
                    logger.error(f"Error while upserting issues with custom fields: {e}")
                    session.rollback()
                    raise

    # NEED TO DRASTICALLY REDUCE THE TIME NEEDED TO UPSERT ACTIVITYITEMS
    #questo metodo che segue è il secondo dei due coinvolti nella prima funzionalità della classe
    #esso ha invece la funzione di sincronizzare verso il database locale lo storico delle
    #modifiche affinchè vengano mappati i cambiamenti di valore nel tempo(più complesso del primo)
    @staticmethod
    def upsert_activity_items(activity_item_data:list):

        icf = aliased(IssueCustomField)

        activity_item_rows = []
        field_value_rows = []
        value_rows = []

        custom_field_id_mapper = load_custom_field_mapper()

        for data in activity_item_data:
            targetMember = data.get('targetMember')

            if targetMember is not None:



                issue = data.get('target')

                issue_id_readable = issue.get('idReadable',None) if issue else None

                rm = data.get('removed')

                added = data.get('added')

                field_name = extract_field_name(targetMember)
                customField_id = custom_field_id_mapper.get(f"{field_name}/{issue_id_readable}",None)


                
                added_uuid = None
                rm_uuid = None
                
                if customField_id:
                    if rm:
                        rm_uuid = uuid.uuid4()

                        field_value_rows.append({
                            "id":rm_uuid
                        })

                        if isinstance(rm,list):
                            for item in rm:
                                try:
                                    value_obj = get_value_obj(item,rm_uuid,field_name) if field_name
                                    value_rows.append(value_obj)
                                except Exception as e:
                                    logger.warning(f"unable to create Value item: {item} / {field_name} / {issue_id_readable}")
                        else:
                            try:
                                rm = get_value_obj(rm,rm_uuid,field_name)
                                value_rows.append(rm)
                            except Exception as e:
                                logger.warning(f"unable to create Value item: {rm} / {field_name} / {issue_id_readable}")

                    if added:
                        added_uuid = uuid.uuid4()

                        field_value_rows.append({
                            "id":added_uuid
                        })
                        if isinstance(added,list):
                            for item in added:
                                try:
                                    value_obj = get_value_obj(item,added_uuid,field_name)
                                    value_rows.append(value_obj)
                                except Exception as e:
                                    logger.warning(f"unable to create Value item: {item} / {field_name} / {issue_id_readable}")
                        else:
                            try:
                                added = get_value_obj(added,added_uuid,field_name)
                                value_rows.append(added)
                            except Exception as e:
                                logger.warning(f"unable to create Value item: {added} / {field_name} / {issue_id_readable}")

                    timestamp = data.get('timestamp')
                    timestamp = datetime.fromtimestamp(timestamp/1000) if timestamp else None
                    activity_item_rows.append({
                        'field_id': customField_id,
                        'old_value_id': rm_uuid,
                        'new_value_id': added_uuid,
                        'timestamp': timestamp
                    })


        insert_field_values_stmt = (
            insert(FieldValue)
            .values(field_value_rows)
        )

        insert_cf_change_stmt = (
            insert(IssueCustomFieldChange)
            .values(activity_item_rows)
            .on_conflict_do_nothing(
                index_elements=["field_id", "timestamp"]
            )
            .returning(IssueCustomFieldChange.id)
        )




        with Session(engine) as session:
            try:
                session.execute(insert_field_values_stmt)

                icfc_id = session.execute(insert_cf_change_stmt).fetchall()

                session.add_all(value_rows)


                logger.debug('worker is done')
                session.commit()

            except Exception as e:
                logger.error(f"Error while upserting data: {e}")
                session.rollback()
                raise
   
    #questo metodo ricostruisce la vita di ogni singolo validation 
    @staticmethod
    def validation_changes():
        
        i = aliased(Issue)
        icf = aliased(IssueCustomField)
        icfc = aliased(IssueCustomFieldChange)
        sv = aliased(StringValue)


        validation_changes_cte = (
            select( # prendo solo i campi rilevanti
                i.parent_id,
                i.id_readable,
                icf.name.label('custom_field_name'),
                icfc.timestamp,
                sv.value.label('custom_field_value')
            )
            .join(icf,i.id_readable == icf.issue_id)
            .join(icfc,icf.id == icfc.field_id)
            .join(sv, icfc.new_value_id == sv.field_id, isouter=True)
            .where(i.summary.like('(Integration Test Verification)%')) # mi interessano solo questi
        ).cte('validation_changes_cte')


        completions_cte = (
            select(
                validation_changes_cte.c.parent_id,
                validation_changes_cte.c.id_readable,
                validation_changes_cte.c.timestamp,
                validation_changes_cte.c.custom_field_value
            )
            .where(
                validation_changes_cte.c.custom_field_name == 'Stage',
                validation_changes_cte.c.custom_field_value.in_(['Done','Blocked']) 
            )
        ).cte('completions_cte')

        last_set_as_done_cte = (
            select(
                completions_cte.c.id_readable,
                func.max(completions_cte.c.timestamp).label("last_set_as_done")
            )
            .group_by(completions_cte.c.id_readable)
        ).cte("last_set_as_done_cte")


        assignements_cte = (
            select(
                validation_changes_cte.c.parent_id,
                validation_changes_cte.c.id_readable,
                validation_changes_cte.c.timestamp,
                validation_changes_cte.c.custom_field_value
            )
            .where(
                validation_changes_cte.c.custom_field_name == 'Assignee',
                validation_changes_cte.c.custom_field_value.in_(TCoE_MEMBERS) 
            )
        ).cte('assignements_cte')

        first_assignements_to_TCoE_cte = (
            select(
                assignements_cte.c.id_readable,
                func.min(assignements_cte.c.timestamp).label('first_assigned_to_TCoE')
            )
            .group_by(assignements_cte.c.id_readable)
        ).cte('first_assignements_to_TCoE_cte')

        latest_assignment_subq = (
            select(
                assignements_cte.c.id_readable.label('id_readable'),
                func.max(assignements_cte.c.timestamp).label('latest_timestamp'),
                completions_cte.c.timestamp
            )
            .where(
                assignements_cte.c.id_readable == completions_cte.c.id_readable,
                assignements_cte.c.timestamp < completions_cte.c.timestamp
            )
            .group_by(assignements_cte.c.id_readable,completions_cte.c.timestamp)
        ).cte('latest_assignment_subq')


        latest_assignment_with_assignee_cte = (
            select(
                latest_assignment_subq.c.id_readable,
                latest_assignment_subq.c.latest_timestamp,
                latest_assignment_subq.c.timestamp,
                assignements_cte.c.custom_field_value.label('assignee')
            )
            .join(assignements_cte, and_(
                latest_assignment_subq.c.id_readable == assignements_cte.c.id_readable,
                latest_assignment_subq.c.latest_timestamp == assignements_cte.c.timestamp
            ), isouter = True)
        ).cte('latest_assignment_with_assignee_cte')


        working_sessions_cte = (
            select(
                completions_cte.c.id_readable,
                completions_cte.c.parent_id,
                completions_cte.c.timestamp,
                completions_cte.c.custom_field_value,
                latest_assignment_with_assignee_cte.c.assignee,
                latest_assignment_with_assignee_cte.c.latest_timestamp.label('assigned_ts')
            )
            .join(latest_assignment_with_assignee_cte, and_(
                completions_cte.c.id_readable == latest_assignment_with_assignee_cte.c.id_readable,
                completions_cte.c.timestamp == latest_assignment_with_assignee_cte.c.timestamp
            ),isouter=True)
        ).cte('working_sessions_cte')


        parent = (
            select(
                icf.issue_id,
                sv.value
            )
            .join(sv, icf.value_id == sv.field_id)
            .where(icf.name == 'Fix versions')
        ).cte('parent')


        stmt = (
            select(
                working_sessions_cte.c.id_readable,
                working_sessions_cte.c.timestamp.label('stop_ts'),
                working_sessions_cte.c.assigned_ts,
                working_sessions_cte.c.custom_field_value,
                working_sessions_cte.c.assignee,
                first_assignements_to_TCoE_cte.c.first_assigned_to_TCoE,
                last_set_as_done_cte.c.last_set_as_done,
                parent.c.value.label('fix_version')
            )
            .join(parent, working_sessions_cte.c.parent_id == parent.c.issue_id, isouter=True)
            .join(first_assignements_to_TCoE_cte, first_assignements_to_TCoE_cte.c.id_readable == working_sessions_cte.c.id_readable,isouter=True)
            .join(last_set_as_done_cte,last_set_as_done_cte.c.id_readable == working_sessions_cte.c.id_readable,isouter=True)
        )

        #working_sessions / completions / assignements
        with Session(engine) as session:
            rows = session.execute(stmt).fetchall()

        sessions_by_assignee = defaultdict(list)

        for row in rows:
            assignee = row[4]
            sessions_by_assignee[assignee].append(row)

        result = []

        for assignee, sessions in sessions_by_assignee.items():

            sessions.sort(key=lambda x: x[1])#ordino le tuple per stop_ts

            timestamps = [s[1] for s in sessions]#prendo solo i timestamp di stop_ts ordinati

            previous_session_stop_ts = None

            for s in sessions:
                if s[2] is None:
                    queue = 0
                else:
                    #ricavo quanti elementi sono compresi tra assigned e stop_ts
                    left = bisect.bisect_right(timestamps, s[2])

                    right = bisect.bisect_left(timestamps, s[1])

                    queue = right - left
                    
                result.append(tuple(s) + (queue,previous_session_stop_ts,))
                
                #setto lo stop_ts di questa sessione come previous_stop_ts della prossima
                previous_session_stop_ts = s[1]

        return result


    @staticmethod
    def okr1():

        i = aliased(Issue)
        icf = aliased(IssueCustomField)
        sv = aliased(StringValue)

        bugs_cte = (
            select(
                i.id_readable,
                i.id,
                func.date_trunc('month',i.created).label('date')
            )
            .join(icf, icf.issue_id == i.id_readable)
            .join(sv, icf.value_id == sv.field_id)
            .where(
                icf.name == 'Type',
                sv.value == 'Bug'
            )
        ).cte('bugs_cte')

        bugs_by_Origine_cte = (
            select(
                bugs_cte.c.id_readable,
                bugs_cte.c.id,
                bugs_cte.c.date,
                sv.value.label('origin')
            )
            .join(icf, bugs_cte.c.id_readable == icf.issue_id)
            .join(sv, icf.value_id == sv.field_id,isouter=True)
            .where(icf.name == 'Origine')
        )

        bugs_by_Origine_and_Product_cte = (
            select(
                bugs_by_Origine_cte.c.id_readable,
                bugs_by_Origine_cte.c.id,
                bugs_by_Origine_cte.c.date,
                bugs_by_Origine_cte.c.origin,
                sv.value.label('product')
            )
            .join(icf, icf.issue_id == bugs_by_Origine_cte.c.id_readable)
            .join(sv, icf.value_id == sv.field_id,isouter=True)
            .where(icf.name == 'Product')           
        ).cte('bugs_by_Origine_and_Product_cte')


        bugs_by_Origine_stmt = (
            select(
                bugs_by_Origine_cte.c.date,
                bugs_by_Origine_cte.c.origin,
                func.count().label('count')
            )
            .group_by(
                bugs_by_Origine_cte.c.date,
                bugs_by_Origine_cte.c.origin
            )
        )

        bugs_by_Origine_and_Product_stmt = (
            select(
                bugs_by_Origine_and_Product_cte.c.date,
                bugs_by_Origine_and_Product_cte.c.origin,
                bugs_by_Origine_and_Product_cte.c.product,
                func.count().label('count')
            )
            .group_by(
                bugs_by_Origine_and_Product_cte.c.date,
                bugs_by_Origine_and_Product_cte.c.product,
                bugs_by_Origine_and_Product_cte.c.origin
            )
        )

        with Session(engine) as session:
            bugs_by_Origine_and_Product = session.execute(bugs_by_Origine_and_Product_stmt).fetchall()

        bug_reports_by_date = {}

        for date,origin,product,count in bugs_by_Origine_and_Product:

            if date not in bug_reports_by_date.keys():
                bug_reports_by_date[date] = {
                    'tot': 0
                }

            bug_reports_by_date[date]['tot'] += count#accumulo tutti i bug

            if origin not in bug_reports_by_date[date].keys():
                bug_reports_by_date[date][origin] = {
                    'tot':0
                }
            bug_reports_by_date[date][origin]['tot'] += count#accumulo tutti i bug segnalati da questa fonte

            if product not in bug_reports_by_date[date][origin].items():
                bug_reports_by_date[date][origin][product] = 0
            
            bug_reports_by_date[date][origin][product] += count#accumulo tutti i bug segnalati da questa fonte per questo prodotto

        grafana_formatted_result = []
        try:
            changelog_releases = ProductRepository.changelog_releases()
        except Exception as e:
            logger.error(e)
            changelog_releases = {}
        for date , bugs_by_origin_and_product in bug_reports_by_date.items():
            if isinstance(bugs_by_origin_and_product,dict):
                customer_bugs = bugs_by_origin_and_product['Cliente']['tot'] if 'Cliente' in bugs_by_origin_and_product.keys() else 0
                grafana_formatted_item = {
                    "date":date,
                    "Customer Bugs":customer_bugs,
                    "Company Bugs":bugs_by_origin_and_product['tot'],#KEY IS MISLEADING; THOSE ARE ALL THE BUGS FOUND BY ANYONE
                    "Defect Rate":customer_bugs / bugs_by_origin_and_product['tot'] if bugs_by_origin_and_product['tot'] > 0 else None,
                    "FW Released": None if (date.year, date.month) in [(d.year, d.month) for d in changelog_releases.values()] else 1
                }

                for origin, products in bugs_by_origin_and_product.items():
                    if isinstance(products,dict):
                        for product, count in products.items():

                            if product != 'tot':

                                product_ratio = count / bugs_by_origin_and_product['tot']

                                grafana_formatted_item[f"{origin}-{product}-ratio"] = product_ratio

                grafana_formatted_result.append(grafana_formatted_item)
        
        return grafana_formatted_result

    #all'interno di questo metodo vengono raggruppati i validation per bucket, calcolati i tempi
    #effettivi di lavorazione e d'attesa in mano al TCoE, viene calcolata la durata totale della
    #fase di test, la durata media di una fase di test per bucket, il tempo d'inattività medio e la 
    #media mobile
    @staticmethod
    def okr2():

        data = get_okr2_data()
        if data:
            return data

        validation_data = IssueRepository.validation_changes()

        rc0_releases = ProductRepository.rc0_releases()
        changelog_releases = ProductRepository.changelog_releases()

        buckets = {}

        for id_readable,stop_ts,assigned_ts,custom_field_value,assignee,first_assigned_to_TCoE,last_set_as_done,fix_version,queue,previous_session_stop_ts in validation_data:
            assigned_ts = assigned_ts if assigned_ts is not None else first_assigned_to_TCoE

            if assigned_ts is None or last_set_as_done is None:
                continue
                #logger.warning(f"smt is wrong {assigned_ts} - {last_set_as_done} / {id_readable}")

            elif fix_version in rc0_releases.keys() and fix_version in changelog_releases.keys():
                if fix_version not in buckets.keys():
                    
                    buckets[fix_version] = {
                        "pre":{
                            "session_count":0,
                            "time_spent":timedelta(0),
                            "idle_time": timedelta(0)
                        },
                        "during":{
                            "session_count":0,
                            "time_spent":timedelta(0),
                            "idle_time": timedelta(0)
                        },
                        "slipped":{
                            "session_count":0,
                            "time_spent":timedelta(0),
                            "idle_time": timedelta(0)
                        },
                        "global":{
                            "session_count":0,
                            "time_spent":timedelta(0),
                            "idle_time": timedelta(0)
                        },
                        "team_members":[]
                    }

                if assignee not in buckets[fix_version]["team_members"]:
                    buckets[fix_version]["team_members"].append(assignee)

                working_session_start = max(date for date in [assigned_ts,previous_session_stop_ts] if date is not None)

                idle_time = working_hours_only_timedelta(working_session_start , assigned_ts)

                rc0_release = convert_to_timezone_aware(rc0_releases[fix_version])


                if working_session_start < rc0_release and stop_ts > rc0_release:
                    pre_fw_session_chunk = working_hours_only_timedelta(rc0_release,working_session_start)
                    during_fw_session_chunk = working_hours_only_timedelta(stop_ts,rc0_release)

                    session_duration = pre_fw_session_chunk + during_fw_session_chunk

                    buckets[fix_version]["pre"]["session_count"] += 1
                    buckets[fix_version]["slipped"]["session_count"] += 1
                    buckets[fix_version]["pre"]["time_spent"] += pre_fw_session_chunk
                    buckets[fix_version]["slipped"]["time_spent"] += during_fw_session_chunk
                    buckets[fix_version]["pre"]["idle_time"] += idle_time

                else:
                    bucket = 'pre' if stop_ts < rc0_release else 'during'  if assigned_ts > rc0_release else 'slipped'
                    
                    session_duration = working_hours_only_timedelta(stop_ts , working_session_start)

                    buckets[fix_version][bucket]["session_count"] += 1
                    buckets[fix_version][bucket]["time_spent"] += session_duration
                    buckets[fix_version][bucket]["idle_time"] += idle_time
                
                buckets[fix_version]['global']['session_count'] +=1
                buckets[fix_version]['global']['time_spent'] += session_duration
                buckets[fix_version]['global']['idle_time'] += idle_time

        okr2_data = []

        for version, value in buckets.items():

            test_phase_end = changelog_releases[version]
            team_members = value.get('team_members',[])
            members_count = len(team_members)

            test_phase_start = rc0_releases[version]

            test_phase_duration = working_hours_only_timedelta(test_phase_end,test_phase_start)

            if test_phase_duration > timedelta(seconds = 0):

                test_phase_working_time = test_phase_duration * members_count #approssimo tutti i membri a full time

                during_time_partition = value["during"]["time_spent"] / test_phase_working_time if test_phase_working_time != timedelta(0) else 0
                slipped_time_partition = value["slipped"]["time_spent"] / test_phase_working_time if test_phase_working_time != timedelta(0) else 0

                test_time_partition_estimate = 1 - (during_time_partition + slipped_time_partition)

                okr2_data.append({
                        "version":version,
                        "date": test_phase_end,
                        "during":during_time_partition,
                        "slipped":slipped_time_partition,
                        "test":test_time_partition_estimate,
                        "duration":test_phase_duration * 3,
                        "raw_duration": test_phase_end - test_phase_start
                    })
            
        for phase in okr2_data:
            date = phase.get('date',None)
            version = phase.get('version',None)

            previous_releases_count = 1
            previous_releases_duration = phase.get('duration')
            for other_phase in okr2_data:
                other_date = other_phase.get('date',None)
                other_version = other_phase.get('version',None)
                other_date_duration = other_phase.get('duration')


                time_diff = date - other_date
                if time_diff > timedelta(0) and time_diff < ITERVALLO_MEDIA_MOBILE and other_date_duration > timedelta(0):
                    previous_releases_duration += other_date_duration
                    previous_releases_count += 1
            phase['average_previous_phase_duration'] = previous_releases_duration / previous_releases_count if previous_releases_count != 0 else 0
        
        set_okr2_data(okr2_data)

        return okr2_data
            
    @staticmethod
    def okr4():
        
        #quick redis check
        data = get_okr4_data()
        if data:
            return data

        validation_data = IssueRepository.validation_changes()

        logger.debug(f"validation_data contains {len(validation_data)} sessions")

        rc0_releases = ProductRepository.rc0_releases()
        #changelog_releases = ProductRepository.changelog_releases()
            
        #uso questo dict per creare la divisione in fw release e buckets
        fix_versions_dict = {}

        validations = {}

        for id_readable,stop_ts,assigned_ts,custom_field_value,assignee,first_assigned_to_TCoE,last_set_as_done,fix_version,queue,previous_session_stop_ts in validation_data:
            if fix_version in rc0_releases.keys():
                rc0_release = convert_to_timezone_aware(rc0_releases[fix_version])
                assigned_ts = convert_to_timezone_aware(
                    assigned_ts
                    ) if assigned_ts else convert_to_timezone_aware(
                        first_assigned_to_TCoE
                        ) if first_assigned_to_TCoE else None

                stop_ts = convert_to_timezone_aware(stop_ts) if stop_ts else None
                previous_session_stop_ts = convert_to_timezone_aware(previous_session_stop_ts) if previous_session_stop_ts else None
                
                if id_readable not in validations.keys():
                    validations[id_readable] = {
                        "first_assigned_to_TCoE":first_assigned_to_TCoE,
                        "last_set_as_Done":last_set_as_done,
                        "fix_version":fix_version,
                        "time_spent":timedelta(0),
                        "idle_time":timedelta(0),
                        "working_sessions":0,
                        "queue":0
                    }

                if assigned_ts is not None:
                    working_session_start = max(date for date in [assigned_ts,previous_session_stop_ts] if date is not None)
                    if working_session_start and stop_ts:
                        validations[id_readable]["time_spent"] += working_hours_only_timedelta(stop_ts,working_session_start)
                        validations[id_readable]["idle_time"] += working_hours_only_timedelta(working_session_start,assigned_ts)
                        validations[id_readable]["working_sessions"] += 1
                        validations[id_readable]["queue"] += queue

                        if assigned_ts < rc0_release and stop_ts > rc0_release:#se la working session è a cavallo dell'rc0 è slipped al TCoE
                            validations[id_readable]['bucket'] = 'slipped_to_TCoE'



        for id_readable,validation_info in validations.items():

            rc0_release = convert_to_timezone_aware(rc0_releases[validation_info["fix_version"]])
            fix_version = validation_info["fix_version"]

            if fix_version not in fix_versions_dict.keys():
                fix_versions_dict[fix_version] = {"tot":{
                    "count":0,
                    "time_spent":timedelta(0),
                    "working_sessions":0,
                    "idle_time":timedelta(0),
                    "queue":0
                    }}
                
            bucket = validation_info.get('bucket',None)
            if not bucket:
                start = validation_info['first_assigned_to_TCoE']
                end = validation_info['last_set_as_Done']
                if start is not None and end is not None:
                    if start < rc0_release and end < rc0_release:
                        bucket = 'pre'
                    elif start > rc0_release and end > rc0_release:
                        bucket = 'during'
                    else:
                        bucket = 'slipped_not_to_TCoE'#nel caso sia uno slipped ma nessuna sessione sia imputabile al TCoE
                else:
                    bucket = None#caso in cui per qualche motivo non ho inizio e/o fine

            if bucket not in fix_versions_dict[fix_version].keys() and bucket is not None:
                fix_versions_dict[fix_version][bucket] = {
                    "count":0,
                    "time_spent":timedelta(0),
                    "working_sessions":0,
                    "idle_time":timedelta(0),
                    "queue":0
                    }
            if bucket is not None:
                fix_versions_dict[fix_version][bucket]["count"] += 1
                fix_versions_dict[fix_version][bucket]["working_sessions"] += validation_info["working_sessions"]
                fix_versions_dict[fix_version][bucket]["time_spent"] += validation_info["time_spent"]
                fix_versions_dict[fix_version][bucket]["idle_time"] += validation_info["idle_time"]
                fix_versions_dict[fix_version][bucket]["queue"] += validation_info["queue"]

            fix_versions_dict[fix_version]["tot"]["count"] += 1
            fix_versions_dict[fix_version]["tot"]["working_sessions"] += validation_info["working_sessions"]
            fix_versions_dict[fix_version]["tot"]["time_spent"] += validation_info["time_spent"]
            fix_versions_dict[fix_version]["tot"]["idle_time"] += validation_info["idle_time"]
            fix_versions_dict[fix_version]["tot"]["queue"] += validation_info["queue"]


        okr4_data = []

        for fix_version, version_info in fix_versions_dict.items():
            grafana_formatted_item = {
                "date": rc0_releases[fix_version],
                "Fix Version": fix_version,
            }

            #calcolo le medie e formatto il risultato per essere facilmente usabile in grafana
            for bucket, bucket_info in version_info.items():
                
                count = bucket_info['count']
                working_sessions = bucket_info["working_sessions"]
                time_spent = bucket_info["time_spent"]
                idle_time = bucket_info["idle_time"]

                validation_average_time_spent = time_spent / count if count > 0 else None
                working_session_average_time_spent = time_spent / working_sessions if working_sessions > 0 else None
                average_idle_time = idle_time / count if count > 0 else None
                average_queue_count = bucket_info["queue"] / count if count > 0 else None

                grafana_formatted_item[f"{bucket}_count"] = count
                grafana_formatted_item[f"{bucket}_time"] = validation_average_time_spent
                grafana_formatted_item[f"{bucket}_session"] = working_session_average_time_spent
                grafana_formatted_item[f"{bucket}_idle"] = average_idle_time
                grafana_formatted_item[f"{bucket}_queue"] = average_queue_count
            
            okr4_data.append(grafana_formatted_item)
        
        #cache results
        set_okr4_data(okr4_data)
    

        return okr4_data



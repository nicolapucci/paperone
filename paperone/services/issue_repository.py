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
    issue_repository gestisce iserimento/lettura/elaborazione dei dati delle Issue principalmente:
        -inserimento e aggiornamento dati
        -analisi e metriche OKR
"""
dev = True

utc = pytz.UTC

TCoE_MEMBERS = ['Sara Tinghi', 'Simona rossi', 'Giuseppe Fragalà', 'Tommaso Capiferri', 'Nicola Montagnani']

ITERVALLO_MEDIA_MOBILE = timedelta(days= (6*30))#6 mesi

avg_val_duration = timedelta(hours=5)

validation_time_share = 0.6 # % of the hours planned to be dedicated to validations, (0.0 - 1)

weekly_working_hours = (40*3 + 32*2)

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

def extract_fw(string:str):
    pattern = r'\d{1,2}\.\d{1,2}\.\d{1,2}'
    match = re.search(pattern,string)
    return match.group(0)

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
def get_value_obj(item, uuid,field_name = None):
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


                tags = data.get('tags')

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
                "updated":updated,
                "tags":tags
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
                                        value_item = get_value_obj(item,new_uuid,None)
                                        value_rows.append(value_item)
                                    except Exception as e:
                                        logger.warning(f"unable to create Value item: {e} -- {item} / {name} / {id_readable}")    
                            else:
                                try:
                                    value_item = get_value_obj(value,new_uuid,None)
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
                        "tags": insert(Issue).excluded.tags
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
    
        logger.debug(f"Received {len(activity_item_data)} ActivityItems")

        activity_item_rows = []
        field_value_rows = []
        value_rows = []

        custom_field_id_mapper = load_custom_field_mapper()

        for data in activity_item_data:
            targetMember = data.get('targetMember')

            if targetMember is None:
                logger.debug(f"{issue.get('idReadable',None)} has no TargetMember")
            else:
                issue = data.get('target')

                issue_id_readable = issue.get('idReadable',None) if issue else None

                rm = data.get('removed')

                added = data.get('added')

                field_name = extract_field_name(targetMember)
                
                customField_id = custom_field_id_mapper.get(f"{field_name}/{issue_id_readable}",None)

                timestamp = data.get('timestamp')
                timestamp = datetime.fromtimestamp(timestamp/1000) if timestamp else None
                
                added_uuid = None
                rm_uuid = None
                
                if customField_id is None:
                    logger.debug(f"unable to find custom field for: {issue_id_readable}-{field_name}")
                else:
                    if not rm and not added:
                        logger.debug(f"{issue_id_readable} \t {field_name} \t {timestamp} added and rm are None \n --- \n {data}")
                    if rm:
                        rm_uuid = uuid.uuid4()

                        field_value_rows.append({
                            "id":rm_uuid
                        })

                        if isinstance(rm,list):
                            for item in rm:
                                try:
                                    value_obj = get_value_obj(item,rm_uuid,field_name)
                                    value_rows.append(value_obj)
                                except Exception as e:
                                    logger.warning(f"unable to create Value item: {item} / {field_name} / {issue_id_readable} \n{e}")
                        else:
                            try:
                                rm = get_value_obj(rm,rm_uuid,field_name)
                                value_rows.append(rm)
                            except Exception as e:
                                logger.warning(f"unable to create Value item: {rm} / {field_name} / {issue_id_readable} \n{e}")

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
                                    logger.warning(f"unable to create Value item: {item} / {field_name} / {issue_id_readable} \n{e}")
                        else:
                            try:
                                added = get_value_obj(added,added_uuid,field_name)
                                value_rows.append(added)
                            except Exception as e:
                                logger.warning(f"unable to create Value item: {added} / {field_name} / {issue_id_readable} \n{e}")

                    
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


                logger.debug(f'Added {len(icfc_id)} ActivityItems')
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
            .join(icfc,icf.id == icfc.field_id, isouter=True)
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

        debug_table = aliased(completions_cte)

        debug_stmt = (
            select(
                func.count(func.distinct(debug_table.c.id_readable)).label('conteggio'),
                parent.c.value.label('fix_version')
            )
            .select_from(debug_table)
            .join(parent, debug_table.c.parent_id == parent.c.issue_id, isouter=True)
            .group_by(parent.c.value)
        )


        #working_sessions / completions / assignements
        with Session(engine) as session:
            rows = session.execute(stmt).fetchall()
            debug_rows = session.execute(debug_stmt).fetchall()
            logger.debug(f" \n\n\n {debug_rows} \n\n\n")
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
        i = aliased(Issue)
        icf = aliased(IssueCustomField)
        icfc = aliased(IssueCustomFieldChange)
        sv = aliased(StringValue)
        tv = aliased(TimeValue)

        data = get_okr2_data()
        if data:
            return data

        validation_data = IssueRepository.validation_changes()

        rc0_releases = ProductRepository.rc0_releases()
        changelog_releases = ProductRepository.changelog_releases()


        bucket_stmt = (
            select(
                i.summary,
                tv.value.label('time_spent')
            )
            .select_from(i)
            .join(icf,i.id_readable == icf.issue_id)
            .join(tv, icf.value_id == tv.field_id)
            .where(
                i.summary.ilike('%validation%during%fw%'),
                icf.name.ilike('%Spent time%')
            )
        )

        buckets = {}

        for id_readable,stop_ts,assigned_ts,custom_field_value,assignee,first_assigned_to_TCoE,last_set_as_done,fix_version,queue,previous_session_stop_ts in validation_data:
            assigned_ts = assigned_ts if assigned_ts is not None else first_assigned_to_TCoE


            if assigned_ts is None or last_set_as_done is None:
                continue
                #logger.warning(f"smt is wrong {assigned_ts} - {last_set_as_done} / {id_readable}")

            elif fix_version in rc0_releases.keys() and fix_version in changelog_releases.keys():
                
                stop_ts = min(stop_ts,convert_to_timezone_aware(changelog_releases[fix_version]))

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

        data = get_okr4_data()
        if data and dev is not True:
            return data

        i = aliased(Issue)
        icf = aliased(IssueCustomField)
        icfc = aliased(IssueCustomFieldChange)
        rm_sv = aliased(StringValue)
        sv = aliased(StringValue)
        tv = aliased(TimeValue)

        rc0_releases = ProductRepository.rc0_releases()

        bucket_stmt = (
            select(
                i.summary,
                tv.value.label('time_spent')
            )
            .select_from(i)
            .join(icf,i.id_readable == icf.issue_id)
            .join(tv, icf.value_id == tv.field_id)
            .where(
                or_(
                    i.summary.ilike('%validation%pre%fw%'),
                    i.summary.ilike('%validation%during%fw%')
                ),
                icf.name.ilike('%Spent time%')
            )
        )

        validations_cte = (
            select(
                i.id_readable,
                i.created
            )
            .select_from(i)
            .where(i.summary.ilike('(Integration Test Verification)%'))
        ).cte('validations_cte')

        parent_fix_version_cte = (
            select(
                i.id_readable,
                sv.value
            )
            .join(icf, i.parent_id == icf.issue_id)
            .join(sv, icf.value_id == sv.field_id)
            .where(icf.name == 'Fix versions')
        ).cte('parent_fix_version_cte')

        completions_cte = (
            select(
                validations_cte.c.id_readable,
                func.max(icfc.timestamp).label('last_set_as_done')
            )
            .join(icf, validations_cte.c.id_readable == icf.issue_id)
            .join(icfc, icf.id == icfc.field_id)
            .join(sv, icfc.new_value_id == sv.field_id)
            .where(
                icf.name == 'Stage',
                or_(
                    sv.value == 'Done',
                    sv.value == 'Blocked'
                )
            )
            .group_by(validations_cte.c.id_readable)
        ).cte('completions_cte')

        first_assignements_cte = (
            select(
                validations_cte.c.id_readable,
                func.min(icfc.timestamp).label('first_assigned')
            )
            .join(icf, validations_cte.c.id_readable == icf.issue_id)
            .join(icfc, icf.id == icfc.field_id)
            .join(sv, icfc.new_value_id == sv.field_id)
            .where(
                icf.name == 'Assignee',
                sv.value.in_(TCoE_MEMBERS)
            )
            .group_by(validations_cte.c.id_readable)
        ).cte('first_assignements_cte')

        validations_stmt = (
            select(
                validations_cte.c.id_readable,
                validations_cte.c.created,
                completions_cte.c.last_set_as_done,
                first_assignements_cte.c.first_assigned,
                parent_fix_version_cte.c.value.label('fix_version'),
                sv.value.label('assignee')
            )
            .select_from(validations_cte)
            .join(first_assignements_cte,validations_cte.c.id_readable == first_assignements_cte.c.id_readable, isouter = True)
            .join(completions_cte, validations_cte.c.id_readable == completions_cte.c.id_readable, isouter=True)
            .join(parent_fix_version_cte, validations_cte.c.id_readable == parent_fix_version_cte.c.id_readable)
            .join(icf, validations_cte.c.id_readable == icf.issue_id)
            .join(sv,icf.value_id == sv.field_id)
            .where(icf.name == 'Assignee')
        )


        validations_stage_changes_cte = (
            select(
                validations_cte.c.id_readable,
                sv.value.label('added'),
                rm_sv.value.label('removed'),
                icfc.timestamp
            )
            .join(icf, validations_cte.c.id_readable == icf.issue_id)
            .join(icfc, icf.id == icfc.field_id)
            .join(sv, icfc.new_value_id == sv.field_id)
            .join(rm_sv,icfc.old_value_id == rm_sv.field_id)
            .where(icf.name == 'Stage')
        ).cte('validations_stage_changes_cte')

        changes_stmt = (
            select(
                validations_stage_changes_cte.c.id_readable,
                validations_stage_changes_cte.c.added,
                validations_stage_changes_cte.c.removed,
                validations_stage_changes_cte.c.timestamp,
                parent_fix_version_cte.c.value.label('fix_version')
            )
            .join(parent_fix_version_cte,validations_stage_changes_cte.c.id_readable == parent_fix_version_cte.c.id_readable)
        )

        with Session(engine) as session:
            buckets = session.execute(bucket_stmt).fetchall()
            validations = session.execute(validations_stmt).fetchall()
            changes = session.execute(changes_stmt).fetchall()

        validation_changes = defaultdict(defaultdict)
        for id_readable,added,removed,timestamp,fix_version in changes:
            if id_readable not in validation_changes[fix_version]:
                validation_changes[fix_version][id_readable] = []
            validation_changes[fix_version][id_readable].append((added,removed,timestamp))
        
        def sum_blocked_time(lista, rc0_release, target='blocked'):
            rc0_release = convert_to_timezone_aware(rc0_release) if rc0_release is not None else None
            totale = timedelta(0)
            target = target.lower()
            aperto = None  # tiene traccia dell'ultimo timestamp per ogni "added"
            lista.sort(key= lambda x:x[2], reverse = False)
            slipped_due_to_block_stage = 0
            for added, removed, t in lista:
                added = added.lower()
                removed = removed.lower()
                if added == target:
                    aperto = t

                if removed == target and aperto is not None:
                    totale += (t - aperto)
                    if rc0_release and t> rc0_release and aperto < rc0_release:#blocked prima di rc0 e sbloccato dopo
                        slipped_due_to_block_stage = 1
                    aperto = None  # garantisce "primo match"
                
            if aperto is not None:
                now = datetime.now(tz=utc)
                totale += now - aperto
            return totale,slipped_due_to_block_stage

        blocked_time_by_fw = defaultdict()
        for fw,val in validation_changes.items():
            blocked_times_and_slipped_due_to_block_stage = [sum_blocked_time(changes,rc0_releases.get(fw,None)) for changes in val.values()]
            blocked_times = [item[0] for item in blocked_times_and_slipped_due_to_block_stage]
            slipped_due_to_block_stage_list = [item[1] for item in blocked_times_and_slipped_due_to_block_stage]
            slipped_due_to_block_stage = sum(slipped_due_to_block_stage_list)
            blocked_time = timedelta(0)
            for time in blocked_times:
                blocked_time += time
            blocked_time_by_fw[fw] = (blocked_time,slipped_due_to_block_stage)
        

        fw_time_spent = defaultdict(timedelta)
        for name, time_spent in buckets:
            fw_time_spent[extract_fw(name)] += time_spent    
        
        
        fw_dict = defaultdict(list)
        ignored_fw_dict = defaultdict(list)
        ids = []
        for id_readable, created, last_set_as_done,first_assigned,fix_version,assignee in validations:
            if  fix_version is not None and assignee in TCoE_MEMBERS:
                if id_readable not in ids:
                    if not first_assigned:
                        first_assigned = created
                        logger.debug(f"{id_readable} has no first ass to TCoE but it's currently assigned to TCoE")
                    last_set_as_done = last_set_as_done if last_set_as_done is not None else convert_to_timezone_aware(datetime.now())
                    fw_dict[fix_version].append((last_set_as_done,first_assigned))
                    ids.append(id_readable)
            else:
                ignored_fw_dict[fix_version].append(id_readable)
                message = "is not assigned to TCoE" if assignee not in TCoE_MEMBERS else "has no fix version"
                #logger.debug(f"{id_readable} {message}") 


        tmp = defaultdict()
        for fw,val in fw_dict.items():
            rc0_release = rc0_releases.get(fw,None)
            rc0_release = convert_to_timezone_aware(rc0_release) if rc0_release is not None else rc0_release
            if fw in fw_time_spent.keys():
                time_spent = fw_time_spent[fw]
                avg_time_spent = time_spent / len(val)

                presumed_overassignements = 0
                val.sort(key= lambda x:x[0], reverse=True)
                if rc0_release:
                    val_assigned_pre = [x[1] for x in val]
                    val_assigned_pre.sort()
                    last_val_pre_index = bisect.bisect_left(val_assigned_pre,rc0_release)
                    val_assigned_pre = val_assigned_pre[:last_val_pre_index]                    
                    for i,assignement in enumerate(val_assigned_pre):
                        assigned_vals = i+1
                        time_left_before_rc0 = working_hours_only_timedelta(rc0_release,assignement)
                        adjusted_time_left = (time_left_before_rc0 / 40)*weekly_working_hours # working hours only timedelta assumes 40 hours weelky from monday to friday (9:00 am -> 18:00 pm)
                        validations_time_partition = adjusted_time_left * validation_time_share # in case we assume that not 100% of the working hours has to be dedicated to validations
                        teorical_max_val = validations_time_partition / avg_val_duration

                        presumed_overassignements = max([(assigned_vals-teorical_max_val),presumed_overassignements]) 
                        

                lifespans = [c-a for c,a in val]
                sum_lifespans = timedelta(0)
                for l in lifespans:
                    sum_lifespans += l
                avg_val_lifespan = sum_lifespans / len(val)
                timestamps = [c for c,a in val]
                total_queue = 0
                buckets = {
                    "pre":0,
                    "during":0,
                    "slip":0
                }
                for v in val:

                    bucket = "pre" if rc0_release is None or v[0] < rc0_release else "during" if v[1] is not None and v[1] > rc0_release else "slip"
                    buckets[bucket] += 1
                    left = bisect.bisect_right(timestamps, v[1])
                    right = bisect.bisect_left(timestamps, v[0])

                    queue = right - left
                    total_queue += queue
                avg_queue = total_queue / len(val)

                ignored_vals = ignored_fw_dict.get(fw,[])
                blocked_time,slipped_due_to_block_stage = blocked_time_by_fw.get(fw,(timedelta(0),0))
                avg_blocked_time = blocked_time / len(val)
                tmp[fw] = {
                    "total_time_spent":time_spent,
                    "avg_time_spent":avg_time_spent,
                    "avg_val_lifespan":avg_val_lifespan,
                    "avg_blocked_time":avg_blocked_time,
                    "avg_waiting_time":max((avg_val_lifespan - avg_time_spent) - avg_blocked_time, timedelta(0)),#somehow some blocked time end up bigger than the total lifepsan
                    "queue":avg_queue,
                    "count":len(val),
                    "ignored_because_first_ass_is_missing":len(ignored_vals)
                }
                buckets['slip'] = max([(buckets.get('slip',0) - presumed_overassignements) - slipped_due_to_block_stage, 0])
                buckets['overassigned'] = presumed_overassignements
                buckets['blocked'] = slipped_due_to_block_stage
                for bucket,count in buckets.items():
                    bucket_share = count/len(val)
                    tmp[fw][bucket] = bucket_share
            #else:
            #    logger.debug(f"No buckets found for fw:{fw}")     
        
        okr4_data = [{"fw":fw,**value} for fw,value in tmp.items()]
        set_okr4_data(okr4_data)
        return okr4_data


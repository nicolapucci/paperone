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
from sqlalchemy.dialects.postgresql import insert as pg_insert

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
    PrimitiveValue,
    ArrayValue,
    DateValue,
    NumberValue,
    StringValue,
    FieldValue,
    Value
)

from services.redis_client import(
    set_okr2_data,
    get_okr2_data,
    set_okr4_data,
    get_okr4_data
)
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
def get_value_obj(item):
    item = item if not isinstance(item,dict) else item.get('name',None)
    if isinstance(item,str):
        return StringValue(value=item)
    elif isinstance(item,int):
        return NumberValue(value=item)
    elif isinstance(item,datetime):
        return DateValue(value=item)#check if it's correct
    return None

#crea un dizionario per ottenere gli id di custom field partendo da CustomField.name e 
#Issue.id_readable
def load_custom_field_mapper(session):
    stmt = (
        select(IssueCustomField.id, IssueCustomField.name, Issue.id_readable)
        .join(Issue, Issue.id == IssueCustomField.issue_id)
    )

    rows = session.execute(stmt).all()

    mapper = {
        (row.name, row.id_readable): row.id
        for row in rows
    }

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
        with Session(engine) as session:
            try:
                len_data = len(issue_data) if issue_data else 0
                logger.info(f"Received {len_data} issues")                
                issues = []
                values_to_add = []
                for data in issue_data:
                    created=convert_to_timestamp(data.get('created'))
                    updated=convert_to_timestamp(data.get('updated'))
                    parent = data.get('parent')
                    parent_issues= parent.get('issues',None) if parent else None
                    
                    parent_issue_id = None
                    if parent_issues:
                        if isinstance(parent_issues, list) and parent_issues:
                            parent_issue_id = parent_issues[0].get('idReadable', None)
                        elif isinstance(parent_issues, dict):
                            parent_issue_id = parent_issues.get('idReadable', None)
                    
                    issue = Issue(
                            youtrack_id=data.get('id'),
                            id_readable=data.get('idReadable'),
                            summary=data.get('summary'),
                            created=created,
                            updated=updated,
                            parent_id=parent_issue_id
                        )

                    for field in data.get('customFields',[]):

                        name = field.get('name')

                        value = field.get('value')
                        
                        if isinstance(value,list):
                            value = ArrayValue(value=[result for item in value if (result := get_value_obj(item)) is not None])
                        else:
                            result = get_value_obj(value)
                            if result is not None:
                                value = PrimitiveValue(value=result)
                            else:
                                value = None 
                        if value:
                            values_to_add.append(value)
                            issueCustomField = IssueCustomField(
                                name=name,
                                value=value,
                                issue=issue
                            )
                            issue.custom_fields.append(issueCustomField)
                    
                    issues.append(issue)
                logger.info(f"Adding {len(values_to_add)} Values")
                session.add_all(values_to_add)
                session.commit()
                for value in values_to_add:
                    session.refresh(value)

                logger.info(f"Upserting {len(issues)} Issues...")

                stmt = (
                    insert(Issue)
                    .values([{
                        'youtrack_id': issue.youtrack_id,
                        'id_readable': issue.id_readable,
                        'summary': issue.summary,
                        'created': issue.created,
                        'updated': issue.updated,
                        'parent_id': issue.parent_id
                    } for issue in issues]
                    ).on_conflict_do_update(
                        index_elements=["youtrack_id"],
                        set_={
                            "summary": insert(Issue).excluded.summary,
                            "updated": insert(Issue).excluded.updated,
                            "parent_id": insert(Issue).excluded.parent_id,
                        }
                    )
                )
                session.execute(stmt)
                session.commit()
                for issue in issues:
                    for custom_field in issue.custom_fields:
                        value_id = custom_field.value.id if custom_field.value else None

                        issue_id_stmt = (
                            select(Issue.id
                            ).select_from(Issue
                            ).where(
                                Issue.id_readable == issue.id_readable
                            )
                        )
                        issue_id = session.execute(issue_id_stmt).scalar_one_or_none()

                        stmt_cf = (
                            insert(IssueCustomField)
                            .values({
                                'name': custom_field.name,
                                'value_id': value_id,
                                'issue_id': issue_id
                            }).on_conflict_do_update(#ut
                                index_elements=["issue_id", "name"],
                                set_={
                                    "value_id": insert(IssueCustomField).excluded.value_id
                                }
                            )
                        )
                        session.execute(stmt_cf)
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

        with Session(engine) as session:
            try:
                logger.info(f"received {len(activity_item_data)} activity items...")

                activity_item_rows = []

                values_to_add = []

                custom_field_id_mapper = load_custom_field_mapper(session)

                for data in activity_item_data:
                    targetMember = data.get('targetMember')

                    if targetMember is not None:
                        issue = data.get('target')
                        try:
                            issue_id_readable = issue.get('idReadable') if issue else None
                        except Exception:
                            logger.debug(f"Cannot get idReadable from {data}")
                            continue
                        
                        rm = data.get('removed')
                        rm = ArrayValue(value=[result for item in rm if (result := get_value_obj(item)) is not None])if isinstance(rm,list) else PrimitiveValue(value=get_value_obj(item=rm))
                        
                        added = data.get('added')
                        added = ArrayValue(value=[result for item in added if (result := get_value_obj(item)) is not None])if isinstance(added,list) else PrimitiveValue(value=get_value_obj(item=added))
                        
                        if rm:
                            values_to_add.append(rm)
                        if added:
                            values_to_add.append(added)

                        field_name = extract_field_name(targetMember)
                        customField_id = custom_field_id_mapper.get((field_name,issue_id_readable),None)

                        #qui creo l'uuid per rm e/o added (se non sono None)

                        if customField_id:
                            try:
                                timestamp = data.get('timestamp')
                                timestamp = datetime.fromtimestamp(timestamp/1000) if timestamp else None
                                activity_item_rows.append({
                                    'field_id': customField_id,
                                    'old_value': rm if rm else None,#qui invece metto direttamente rm.id,added.id o None
                                    'new_value': added if added else None,
                                    'timestamp': timestamp
                                })
                            except Exception as e:
                                logger.error(f"Error while creating activity_item: {field_name}, {issue_id_readable},{rm},{added},{data.get('timestamp')}")
                                raise
                        #else:
                            #logger.warning(f"Custom field {field_name} -- {targetMember} of issue {issue_id_readable} not found")

                added_items = 0

                session.add_all(values_to_add)
                session.commit()#rimuovo questo commit
                for item in values_to_add:#non faccio il
                    session.refresh(item)

                if activity_item_rows:
                    logger.info(f"trying to add {len(activity_item_rows)} activity Items")
    
                    activity_item_rows = [{
                                    'field_id': item['field_id'],
                                    'old_value_id': item['old_value'].id if item['old_value'] else None,
                                    'new_value_id': item['new_value'].id if item['new_value'] else None,
                                    'timestamp': item['timestamp']
                                } for item in activity_item_rows]

                    stmt_cf_change = pg_insert(IssueCustomFieldChange
                        ).values(activity_item_rows
                        ).on_conflict_do_nothing(
                        index_elements=["field_id", "timestamp"]
                    ).returning(IssueCustomFieldChange.id)
                    result = session.execute(stmt_cf_change).fetchall()
                    added_items = len(result)

                session.commit()    
                logger.info(f"added {added_items} new activity Items")
            
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
            .join(icf,i.id == icf.issue_id)
            .join(icfc,icf.id == icfc.field_id)
            .join(sv, icfc.new_value_id == sv.field_id)
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
            ))
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


        current_session = aliased(working_sessions_cte)
        previous_sessions = aliased(working_sessions_cte)


        parent = (
            select(
                i.id_readable,
                sv.value
            )
            .join(icf,i.id == icf.issue_id)
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
            .join(parent, working_sessions_cte.c.parent_id == parent.c.id_readable, isouter=True)
            .join(first_assignements_to_TCoE_cte, first_assignements_to_TCoE_cte.c.id_readable == working_sessions_cte.c.id_readable)
            .join(last_set_as_done_cte,last_set_as_done_cte.c.id_readable == working_sessions_cte.c.id_readable,isouter=True)

            #.where(working_sessions_cte.c.assignee == 'Simona rossi' , parent.c.value == '4.19.0')

        )
        with Session(engine) as session:
            rows = session.execute(stmt).fetchall()
            logger.debug(f"working_sessions: {len(rows)}")



        sessions_by_assignee = defaultdict(list)

        for row in rows:
            assignee = row[4]
            sessions_by_assignee[assignee].append(row)

        result = []

        for assignee, sessions in sessions_by_assignee.items():

            sessions.sort(key=lambda x: x[1])

            timestamps = [s[1] for s in sessions]

            previous_session_stop_ts = None

            for s in sessions:
                if s[2] is None:
                    queue = 0
                else:
                    left = bisect.bisect_right(timestamps, s[2])

                    right = bisect.bisect_left(timestamps, s[1])

                    queue = right - left
                    

                result.append(tuple(s) + (queue,previous_session_stop_ts,))

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
            .join(icf, icf.issue_id == i.id)
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
            .join(icf, icf.issue_id == bugs_cte.c.id)
            .join(sv, icf.value_id == sv.field_id)
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
            .join(icf, icf.issue_id == bugs_by_Origine_cte.c.id)
            .join(sv, icf.value_id == sv.field_id)
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

        changelog_releases = ProductRepository.changelog_releases()

        for date , bugs_by_origin_and_product in bug_reports_by_date.items():
            if isinstance(bugs_by_origin_and_product,dict):
                customer_bugs = bugs_by_origin_and_product['Cliente']['tot'] if 'Cliente' in bugs_by_origin_and_product.keys() else 0
                grafana_formatted_item = {
                    "date":date,
                    "Customer Bugs":customer_bugs,
                    "Company Bugs":bugs_by_origin_and_product['tot'],
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
            if fix_version in rc0_releases.keys():
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
                    
                    #logger.debug(f"logging in {bucket}-- {stop_ts} | {rc0_release} | {working_session_start}")

                    session_duration = working_hours_only_timedelta(stop_ts , working_session_start)

                    buckets[fix_version][bucket]["session_count"] += 1
                    buckets[fix_version][bucket]["time_spent"] += session_duration
                    buckets[fix_version][bucket]["idle_time"] += idle_time
                
                buckets[fix_version]['global']['session_count'] +=1
                buckets[fix_version]['global']['time_spent'] += session_duration
                buckets[fix_version]['global']['idle_time'] += idle_time

        okr2_data = []

        for version, value in buckets.items():
            print(f"parsing {version}")

            test_phase_end = changelog_releases[version] if version in changelog_releases.keys() else rc0_releases[version]

            team_members = value.get('team_members',[])
            members_count = len(team_members)

            test_phase_start = rc0_releases[version] if version in rc0_releases.keys() else changelog_releases[version]

            test_phase_duration = working_hours_only_timedelta(test_phase_end,test_phase_start)

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

            previous_releases_count = 0
            previous_releases_duration = timedelta(0)
            for other_phase in okr2_data:
                other_date = other_phase.get('date',None)
                other_version = other_phase.get('version',None)
                other_date_duration = other_phase.get('duration')


                time_diff = date - other_date
                if time_diff > timedelta(0) and time_diff < ITERVALLO_MEDIA_MOBILE:
                    previous_releases_duration += other_date_duration
                    previous_releases_count += 1
            phase['average_previous_phase_duration'] = previous_releases_duration / previous_releases_count if previous_releases_count != 0 else 0
        
        set_okr2_data(okr2_data)

        return okr2_data
            
    @staticmethod
    def okr4():
        
        data = get_okr4_data()
        if data:
            return data
        validation_data = IssueRepository.validation_changes()

        rc0_releases = ProductRepository.rc0_releases()
        changelog_releases = ProductRepository.changelog_releases()

        validations = {}

        for id_readable,stop_ts,assigned_ts,custom_field_value,assignee,first_assigned_to_TCoE,last_set_as_done,fix_version,queue,previous_session_stop_ts in validation_data:
            if fix_version in rc0_releases.keys():
                rc0_release = convert_to_timezone_aware(rc0_releases[fix_version])
                assigned_ts = convert_to_timezone_aware(assigned_ts) if assigned_ts else None
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

                        if assigned_ts < rc0_release and stop_ts > rc0_release:
                            validations[id_readable]['bucket'] = 'slipped_to_TCoE'


            fix_versions_dict = {}
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

                    if start < rc0_release and end < rc0_release:
                        bucket = 'pre'
                    elif start > rc0_release and end > rc0_release:
                        bucket = 'during'
                    else:
                        bucket = 'slipped_not_to_TCoE'

                if bucket not in fix_versions_dict[fix_version].keys():
                    fix_versions_dict[fix_version][bucket] = {
                        "count":0,
                        "time_spent":timedelta(0),
                        "working_sessions":0,
                        "idle_time":timedelta(0),
                        "queue":0
                        }

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
                "date": changelog_releases[fix_version] if fix_version in changelog_releases.keys() else rc0_releases[fix_version],
                "Fix Version": fix_version,
            }

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
        
        set_okr4_data(okr4_data)

        return okr4_data



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

import pandas as pd

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
import os

"""
    issue_repository gestisce iserimento/lettura/elaborazione dei dati delle Issue principalmente:
        -inserimento e aggiornamento dati
        -analisi e metriche OKR
"""
dev = os.getenv('dev')

utc = pytz.UTC

TCoE_MEMBERS = ['Sara Tinghi', 'Simona rossi', 'Giuseppe Fragalà', 'Tommaso Capiferri', 'Nicola Montagnani']

ITERVALLO_MEDIA_MOBILE = timedelta(days= (6*30))#6 mesi

avg_val_duration = timedelta(hours=5)

validation_time_share = 0.6 # % of the hours planned to be dedicated to validations, (0.0 - 1)

weekly_working_hours = (40*3 + 32*2)


# TODO escludere giorni festivi, se possibile usare i giorni lavorativi effettivi
def working_hours_only_timedelta(end_date:datetime,start_date:datetime):
    end_date = end_date.replace(tzinfo=utc)
    start_date = start_date.replace(tzinfo=utc)

    current_date = start_date

    working_time = timedelta(0)

    while current_date < end_date:
        if current_date.weekday() < 5:

            working_day_start = datetime(current_date.year,current_date.month,current_date.day,8,0,0).replace(tzinfo=utc)
            working_day_break_start = datetime(current_date.year,current_date.month,current_date.day,12,0,0).replace(tzinfo=utc)
            working_day_break_end = datetime(current_date.year,current_date.month,current_date.day,13,0,0).replace(tzinfo=utc)
            working_day_end = datetime(current_date.year,current_date.month,current_date.day,17,0,0).replace(tzinfo=utc)

            start = max(current_date, working_day_start)
            end = min(end_date, working_day_end)

            if start < end:
                if start > working_day_break_end or end < working_day_break_start:
                    working_time += (end-start)
                else:
                    working_time += ((end - start) - timedelta(hours=1))

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

    @staticmethod
    def get_validation_ids(last_activities_pull=None):
        if last_activities_pull is None:
            stmt = (
                select(Issue.youtrack_id)
                .where(Issue.summary.ilike('%(Integration Test Verification)%'))
            )
        else:
            stmt = (
                select(Issue.youtrack_id)
                .where(Issue.summary.ilike('%(Integration Test Verification)%'))
                .where(Issue.updated >= last_activities_pull)
            )
        with Session(engine) as session:
            r = session.execute(stmt).fetchall()
        
        return [i[0] for i in r]
    
    #questo metodo che segue è uno dei due coinvolti per la prima funzionalità della classe cui fa 
    #parte
    #il metodo ha il compito di sincronizzare i dati delle issue verso il database locale
    #se l'issue esiste già viene aggiornata altrimenti viene creata
    @staticmethod
    def upsert_issues(issue_data:list):

        if not issue_data:
            logger.warning(f"Received no Issue data :{issue_data}")
            raise ValueError(f"No issue data provided")

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
                tag_names = []
                for tag in tags:
                    tag_name = tag.get('name')
                    if tag_name:
                        tag_names.append(tag_name)
                    else:
                        logger.debug(f"received tag with no attribute name: {tag}")

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
                "tags":tag_names
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
                    

                    custom_field_inserted = session.execute(upsert_custom_fields_stmt).fetchall()

                    logger.info(f"Added/Updated {len(issue_inserted)} Issues and {len(custom_field_inserted)} Custom Fields")
                    session.commit() 


                except Exception as e:
                    logger.error(f"Error while upserting issues with custom fields: {e}")
                    session.rollback()
                    raise

    #questo metodo che segue è il secondo dei due coinvolti nella prima funzionalità della classe
    #esso ha invece la funzione di sincronizzare verso il database locale lo storico delle
    #modifiche affinchè vengano mappati i cambiamenti di valore nel tempo(più complesso del primo)
    @staticmethod
    def upsert_activity_items(activity_item_data:list):
    
        activity_item_rows = []
        field_value_rows = []
        value_rows = []

        custom_field_id_mapper = load_custom_field_mapper()

        for data in activity_item_data:
            targetMember = data.get('targetMember')

            if targetMember is None:
                logger.error(f"{issue.get('idReadable',None)} has no TargetMember, skipping this activityItem...")
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
                    logger.error(f"unable to find custom field for: {issue_id_readable}-{field_name}, skipping this activityItem...")
                else:
                    if not rm and not added:
                        logger.warning(f"{issue_id_readable} \t {field_name} \t {timestamp} added and rm are None")
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
        try:
            with Session(engine) as session:
                bugs_by_Origine_and_Product = session.execute(bugs_by_Origine_and_Product_stmt).fetchall()
        except Exception as e:
            logger.error(f"Error permorming okr1 query: {e}")
            raise

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
            logger.error(f"Unable to retrieve changelog releases, will not report correctly if a firmware was released.")
            changelog_releases = {}

        for date , bugs_by_origin_and_product in bug_reports_by_date.items():
            if isinstance(bugs_by_origin_and_product,dict):
                customer_bugs = bugs_by_origin_and_product['Cliente']['tot'] if 'Cliente' in bugs_by_origin_and_product.keys() else 0
                grafana_formatted_item = {
                    "date":date,
                    #"Customer Bugs":customer_bugs,
                    #"Company Bugs":bugs_by_origin_and_product['tot'],#KEY IS MISLEADING; THOSE ARE ALL THE BUGS FOUND BY ANYONE
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
        tv = aliased(TimeValue)

        data = get_okr2_data()
        if data and dev is not True:
            return data
        rc0_releases = ProductRepository.rc0_releases()

        try:
            changelog_releases = ProductRepository.changelog_releases()
        except Exception as e:
            changelog_releases = {}
            logger.error(f"Unable to retrieve changelogs from wiki, test phase duration will be set to 0...")

        during_fw_stmt = (
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

        manual_tests_stmt  = (
            select(
                i.summary,
                tv.value.label('time_spent')
            )
            .select_from(i)
            .join(icf,i.id_readable == icf.issue_id)
            .join(tv, icf.value_id == tv.field_id)
            .where(
                i.summary.ilike('test kalliope% - manual testing'),
                icf.name.ilike('%Spent time%')
            )
        )

        automated_tests_stmt = (
            select(
                i.summary,
                tv.value.label('time_spent')
            )
            .select_from(i)
            .join(icf,i.id_readable == icf.issue_id)
            .join(tv, icf.value_id == tv.field_id)
            .where(
                i.summary.ilike('test kalliope% - automated testing'),
                icf.name.ilike('%Spent time%')
            )
        )

        try:
            with Session(engine) as session:
                validation_during_fw = session.execute(during_fw_stmt).fetchall()
                automated_tests = session.execute(automated_tests_stmt).fetchall()
                manual_tests = session.execute(manual_tests_stmt).fetchall()
        except Exception as e:
            raise

        validation_time_spent = {extract_fw(i[0]):i[1] for i in validation_during_fw}
        manual_tests_time_spent = {extract_fw(i[0]):i[1] for i in manual_tests}
        automated_tests_time_spent = {extract_fw(i[0]):i[1] for i in automated_tests}


        okr2_data = []
        tmp = []
        for fw,rc0_release in rc0_releases.items():
            start = rc0_release
            end = changelog_releases[fw] if fw in changelog_releases.keys() else None
            validations = validation_time_spent[fw] if fw in validation_time_spent.keys() else timedelta(0)
            manual = manual_tests_time_spent[fw] if fw in manual_tests_time_spent.keys() else timedelta(0)
            automated = automated_tests_time_spent[fw] if fw in automated_tests_time_spent.keys() else timedelta(0)

            test_phase_duration = end - start if end is not None else timedelta(0)
            test_phase_working_hours_duration = working_hours_only_timedelta(end,start) if end is not None else timedelta(0)
            test_phase_team_working_hours_duration = test_phase_working_hours_duration * (weekly_working_hours / 40)

            phase_info = {
                "fw":fw,
                "start":start,
                #"end":end,
                "test_phase_duration":test_phase_duration,
                #"test_phase_team_working_hours_duration": test_phase_team_working_hours_duration
            }
            if start is not None:
                tmp.append((fw,test_phase_duration,start))

            if test_phase_team_working_hours_duration > timedelta(0):
                phase_info["validations_time_share"] = validations / test_phase_team_working_hours_duration
                phase_info["manual_time_share"] = manual / test_phase_team_working_hours_duration
                phase_info["automated_time_share"] = automated / test_phase_team_working_hours_duration
                phase_info["other"] = 1- ((validations + manual + automated) / test_phase_team_working_hours_duration)
            okr2_data.append(phase_info)

        df = pd.DataFrame(okr2_data)



        df["start"] = pd.to_datetime(df["start"])
        df = df.sort_values("start")

        window_mesi = 3
        medie = []

        for _, row in df.iterrows():
            fine = row["start"]
            inizio = fine - pd.DateOffset(months=window_mesi)
            
            subset = df[
                (df["start"] < fine) &   # esclude fw corrente
                (df["start"] >= inizio)
            ]
            
            media = subset["test_phase_duration"].mean()
            medie.append(media)

        df[f"media_a_{window_mesi}_mesi"] = medie

        okr2_data = df.to_dict(orient="records")

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
        try:
            with Session(engine) as session:
                buckets = session.execute(bucket_stmt).fetchall()
                validations = session.execute(validations_stmt).fetchall()

                changes = session.execute(changes_stmt).fetchall()
        except Exception as e:
            raise

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
                        logger.warning(f"{id_readable} has no first ass to TCoE but it's currently assigned to TCoE")
                    last_set_as_done = last_set_as_done if last_set_as_done is not None else convert_to_timezone_aware(datetime.now())
                    fw_dict[fix_version].append((last_set_as_done,first_assigned))
                    ids.append(id_readable)
            else:
                ignored_fw_dict[fix_version].append(id_readable)
                message = "is not assigned to TCoE" if assignee not in TCoE_MEMBERS else "has no fix version"
                logger.debug(f"{id_readable} {message}") 


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
                    avg_waiting_time = max((avg_val_lifespan - avg_time_spent) - avg_blocked_time, timedelta(0))
                    tmp[fw] = {
                        "date":rc0_release,
                        #"total_time_spent":time_spent,
                        "avg_time_spent":avg_time_spent,
                        "time_spent_share": avg_time_spent / avg_val_lifespan,
                        "blocked_share": avg_blocked_time / avg_val_lifespan,
                        "waiting_share":avg_waiting_time / avg_val_lifespan,
                        "avg_val_lifespan":avg_val_lifespan,
                        #"avg_blocked_time":avg_blocked_time,
                        #"avg_waiting_time":avg_waiting_time,
                        "queue":avg_queue,
                        "count":len(val),
                        #"ignored_because_first_ass_is_missing":len(ignored_vals)
                    }
                    buckets['slip'] = max([(buckets.get('slip',0) - presumed_overassignements) - slipped_due_to_block_stage, 0])
                    buckets['overassigned'] = presumed_overassignements
                    buckets['blocked'] = slipped_due_to_block_stage
                    for bucket,count in buckets.items():
                        bucket_share = count/len(val)
                        tmp[fw][bucket] = bucket_share
                else:
                    logger.warning(f"Ignoring fw:\t{fw}\t because it has no rc0 Release date")
            else:
                logger.warning(f"Ignoring fw:\t{fw}\t because it has no buckets")
        okr4_data = [{"fw":fw,**value} for fw,value in tmp.items()]
        okr4_data.sort(key= lambda x:x['date'], reverse=True)
        set_okr4_data(okr4_data)
        return okr4_data


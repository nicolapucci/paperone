from sqlalchemy import (
    select,
    exists,
    and_ ,
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
    get_prova_data,
    set_prova_data
)



utc = pytz.UTC

BATCH_SIZE = 1000

TCoE_MEMBERS = ['Sara Tinghi', 'Simona rossi', 'Giuseppe Fragalà', 'Tommaso Capiferri', 'Nicola Montagnani']

def convert_to_timestamp(date):
    return datetime.fromtimestamp(date/1000,tz=timezone.utc)

def extract_field_name(targetMember:str):

    match = re.search(r'__CUSTOM_FIELD__(\w+(?: \w+)*)_\d+', targetMember)

    if match:
        field_name = match.group(1)
        return field_name
    else:
        return None

def get_value_obj(item):
    item = item if not isinstance(item,dict) else item.get('name',None)
    if isinstance(item,str):
        return StringValue(value=item)
    elif isinstance(item,int):
        return NumberValue(value=item)
    elif isinstance(item,datetime):
        return DateValue(value=item)#check if it's correct
    return None

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

    @staticmethod
    def get_max_updated_issue():
        stmt = select(func.max(Issue.updated))

        with Session(engine) as session:
            max_updated = session.execute(stmt).scalar_one_or_none()
        return max_updated.strftime('%Y-%m') if max_updated else None

    # CHECK WHY SOME CUSTOM FIELDS ARE LOST (time_left, spent_time , estimation // prob due to the type of the field)
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
                            }).on_conflict_do_update(
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

    @staticmethod
    def count_reported_bugs():

        icf = aliased(IssueCustomField)
        fv = aliased(FieldValue)
        sv = aliased(StringValue)

        type_exists = (
            select(1)
            .select_from(icf)
            .join(sv, icf.id == sv.field_id)
            .where(
                icf.issue_id == Issue.id,
                icf.name == "Type",
                sv.value == "Bug",
            )
            .exists()
        )

        origine_exists = (
            select(1)
            .select_from(icf)
            .join(sv, icf.value_id ==  sv.field_id)
            .where(
                icf.issue_id == Issue.id,
                icf.name == "Origine",
                sv.value == "Cliente"
            )
            .exists()
        )

        issue_month = func.date_trunc("month", Issue.created)
        customer_bugs_stmt = (
            select(issue_month.label("month"),func.count().label("count"))
        ).select_from(Issue
        ).where(type_exists,origine_exists
        ).group_by(func.date_trunc("month",Issue.created)
        ).order_by(func.date_trunc("month",Issue.created))

        bugs_stmt = (
            select(issue_month.label("month"),func.count().label("count"))
        ).select_from(Issue
        ).where(type_exists
        ).group_by(func.date_trunc("month",Issue.created)
        ).order_by(func.date_trunc("month",Issue.created))

        with Session(engine) as session:
            customer_reported_bugs = session.execute(customer_bugs_stmt).all()
            bugs = session.execute(bugs_stmt).all()
        
        return customer_reported_bugs,bugs

    @staticmethod
    def count_reported_bugs_group_by_product():

        icf = aliased(IssueCustomField)
        fv = aliased(FieldValue)
        sv = aliased(StringValue)

        type_exists = (
            select(1)
            .select_from(icf
            ).join(sv, icf.value_id == sv.field_id
            ).where(
                icf.issue_id == Issue.id,
                icf.name == "Type",
                sv.value == "Bug",
            )
            .exists()
        )

        origine_exists = (
            select(1)
            .select_from(icf)
            .join(sv, icf.value_id ==  sv.field_id)
            .where(
                icf.issue_id == Issue.id,
                icf.name == "Origine",
                sv.value == "Cliente"
            )
            .exists()
        )

        issue_month = func.date_trunc("month", Issue.created)
        customer_bugs_stmt = (
            select(issue_month.label("month"),sv.value,func.count().label("count"))
        ).select_from(Issue
        ).join(icf, Issue.id == icf.issue_id
        ).join(sv, icf.value_id == sv.field_id
        ).where(type_exists,origine_exists,icf.name == "Product"
        ).group_by(func.date_trunc("month",Issue.created),sv.value
        ).order_by(func.date_trunc("month",Issue.created))

        bugs_stmt = (
            select(issue_month.label("month"),sv.value,func.count().label("count"))
        ).select_from(Issue
        ).join(icf, Issue.id == icf.issue_id
        ).join(sv, icf.value_id == sv.field_id
        ).where(type_exists,icf.name == "Product"
        ).group_by(func.date_trunc("month",Issue.created),sv.value
        ).order_by(func.date_trunc("month",Issue.created))

        with Session(engine) as session:
            customer_reported_bugs = session.execute(customer_bugs_stmt).all()
            bugs = session.execute(bugs_stmt).all()
        
        return customer_reported_bugs,bugs

    @staticmethod
    def defect_rate():
        (customer_reported_bugs,bugs) = IssueRepository.count_reported_bugs()
        customer_bugs_dict = {(month.timestamp()*1000):count for month,count in customer_reported_bugs}
        (customer_reported_bugs_by_product,bugs_by_product) = IssueRepository.count_reported_bugs_group_by_product()
        customer_bugs_dict_by_product = {}

        for month,product,count in customer_reported_bugs_by_product:
            month= month.timestamp()*1000
            product = product if product else 'Unknown'
            customer_bugs_dict_by_product[f"{month}{product}"] = count
        
        ratios = {}
        for month,total_count in bugs:
            month= month.timestamp()*1000
            if total_count > 0:
                customer_count = customer_bugs_dict.get(month,0)
                
                ratio = customer_count / total_count if total_count else 0
                
                if month not in ratios:
                    ratios[month]= {}
                ratios[month]['Total'] = ratio

        for month,product,count in bugs_by_product:
            month= month.timestamp()*1000
            if count> 0:
                product = product if product else 'Unknown'
                customer_count = customer_bugs_dict_by_product.get(f"{month}{product}",0)

                ratio = customer_count / count if total_count else 0

                if month not in ratios:
                    ratios[month] = {}

                ratios[month][product]= ratio
      
        results = [{'date': key, 'values': value} for key, value in ratios.items()]
        return results

    @staticmethod
    def prova_2():
        
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

        latest_assignment_subq = (
            select(
                assignements_cte.c.id_readable.label('id_readable'),
                func.max(assignements_cte.c.timestamp).label('latest_timestamp'),
                completions_cte.c.timestamp
            )
            .where(
                assignements_cte.c.id_readable == completions_cte.c.id_readable,
                assignements_cte.c.timestamp <= completions_cte.c.timestamp
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
            ))
        ).cte('working_sessions_cte')

        progress_cte = (
            select(
                validation_changes_cte.c.id_readable,
                validation_changes_cte.c.timestamp,
            )
            .where(
                validation_changes_cte.c.custom_field_name == 'Stage',
                validation_changes_cte.c.custom_field_value == 'In Progress'
            )
        ).cte('progress_cte')

        working_sessions_including_is_progress_changes_cte = (
            select(
                working_sessions_cte.c.id_readable,
                working_sessions_cte.c.parent_id,
                working_sessions_cte.c.timestamp,
                working_sessions_cte.c.custom_field_value,
                working_sessions_cte.c.assignee,
                working_sessions_cte.c.assigned_ts,
                func.max(progress_cte.c.timestamp)
            )
            .join(progress_cte, working_sessions_cte.c.id_readable == progress_cte.c.id_readable)
            .where(working_sessions_cte.c.timestamp > progress_cte.c.timestamp)
            .group_by(
                working_sessions_cte.c.id_readable,
                working_sessions_cte.c.parent_id,
                working_sessions_cte.c.timestamp,
                working_sessions_cte.c.custom_field_value,
                working_sessions_cte.c.assignee,
                working_sessions_cte.c.assigned_ts     
            )
        ).cte('working_sessions_including_is_progress_changes_cte')

        current_session = aliased(working_sessions_including_is_progress_changes_cte)
        previous_sessions = aliased(working_sessions_including_is_progress_changes_cte)

        supposed_working_sessions_cte = (
            select(
                current_session.c.id_readable,
                current_session.c.parent_id,
                current_session.c.timestamp,
                current_session.c.custom_field_value,
                current_session.c.assignee,
                current_session.c.assigned_ts,
                func.max(previous_sessions.c.timestamp).label('previous_session_stop_ts')
            )
            .join(previous_sessions, current_session.c.assignee == previous_sessions.c.assignee)
            .where(previous_sessions.c.timestamp < current_session.c.timestamp)
            .group_by(
                current_session.c.id_readable,
                current_session.c.parent_id,
                current_session.c.timestamp,
                current_session.c.custom_field_value,
                current_session.c.assignee,
                current_session.c.assigned_ts,
            )
        ).cte('supposed_working_sessions_cte')


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
                supposed_working_sessions_cte.c.id_readable,
                supposed_working_sessions_cte.c.timestamp.label('stop_ts'),
                supposed_working_sessions_cte.c.assigned_ts,
                supposed_working_sessions_cte.c.custom_field_value,
                supposed_working_sessions_cte.c.assignee,
                supposed_working_sessions_cte.c.previous_session_stop_ts,
                parent.c.value.label('fix_version'),
            )
            .join(parent, supposed_working_sessions_cte.c.parent_id == parent.c.id_readable)
            #.where(supposed_working_sessions_cte.c.assignee == 'Simona rossi' , parent.c.value == '4.19.0')

        )

        with Session(engine) as session:
            result = session.execute(stmt).fetchall()

        return result


if __name__ =='__main__':
    print(IssueRepository.prova_2())
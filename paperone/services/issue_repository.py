from sqlalchemy import (
    select,
    exists,
    and_ ,
    func,
)
from sqlalchemy.orm import Session, aliased
from sqlalchemy.dialects.postgresql import insert

import re
from sqlalchemy.dialects.postgresql import insert as pg_insert
from services.postgres_engine import engine
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

from datetime import (
    datetime,
    timezone,
    timedelta
)
from services.logger import logger

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

class IssueRepository:

    @staticmethod
    def get_max_updated_issue():
        stmt = select(func.max(Issue.updated))

        with Session(engine) as session:
            max_updated = session.execute(stmt).scalar_one_or_none()
        return max_updated.strftime('%Y-%m') if max_updated else None

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

    @staticmethod
    def upsert_activity_items(activity_item_data:list):
        icf = aliased(IssueCustomField)

        with Session(engine) as session:
            try:
                logger.info(f"received {len(activity_item_data)} activity items...")

                activity_item_rows = []

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
                        
                        session.add(added)
                        session.add(rm)
                        session.commit()
                        session.refresh(added)
                        session.refresh(rm)

                        get_custom_field_stmt = (
                            select(icf
                            ).select_from(icf
                            ).join(Issue, Issue.id == icf.issue_id
                            ).where(
                                icf.name==extract_field_name(targetMember),
                                Issue.id_readable==issue_id_readable
                                )
                        )
                        customField = session.execute(get_custom_field_stmt).scalar_one_or_none()

                        if customField:
                            try:
                                timestamp = data.get('timestamp')
                                timestamp = datetime.fromtimestamp(timestamp/1000) if timestamp else None
                                activity_item_rows.append({
                                    'field_id': customField.id,
                                    'old_value_id': rm.id if rm else None,
                                    'new_value_id': added.id if added else None,
                                    'timestamp': timestamp
                                })
                            except Exception as e:
                                logger.error(f"Error while creating activity_item: {extract_field_name(targetMember)}, {issue_id_readable},{rm},{added},{data.get('timestamp')}")
                                raise
                        else:
                            logger.warning(f"Custom field {extract_field_name(targetMember)} -- {targetMember} of issue {issue_id_readable} not found")
                added_items = 0

                if activity_item_rows:
                    logger.info(f"trying to add {len(activity_item_rows)} activity Items")
    
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
    def average_validation_duration():

        string_value = aliased(StringValue)
        string_value1 = aliased(StringValue)
        string_value2 = aliased(StringValue)
        string_value3 = aliased(StringValue)
        parent_alias = aliased(Issue)
        child_alias = aliased(Issue)
        icf = aliased(IssueCustomField)
        value = aliased(Value)


        first_assignments = (
            select(
                Issue.id_readable,
                IssueCustomFieldChange.timestamp,
                func.row_number().over(
                    partition_by=Issue.id_readable,
                    order_by=IssueCustomFieldChange.timestamp
                ).label('rn')
            )
            .join(IssueCustomField, Issue.id == IssueCustomField.issue_id)
            .join(IssueCustomFieldChange, IssueCustomField.id == IssueCustomFieldChange.field_id)
            .join(FieldValue, FieldValue.id == IssueCustomFieldChange.new_value_id)
            .join(Value, Value.field_id == FieldValue.id)
            .outerjoin(string_value, string_value.id == Value.id)
            .where(
                Issue.summary.like('(Integration Test Verification)%'),
                IssueCustomField.name == 'Assignee',
                string_value.value.in_(TCoE_MEMBERS)
            )
            .cte('first_assignments')
        )

        last_completed = (
            select(
                Issue.id_readable,
                IssueCustomFieldChange.timestamp.label('completed_timestamp'),
                func.row_number().over(
                    partition_by=Issue.id_readable,
                    order_by=IssueCustomFieldChange.timestamp.desc()
                ).label('sn')
            )
            .join(IssueCustomField, Issue.id == IssueCustomField.issue_id)
            .join(IssueCustomFieldChange, IssueCustomField.id == IssueCustomFieldChange.field_id)
            .join(FieldValue, FieldValue.id == IssueCustomFieldChange.new_value_id)
            .join(Value, Value.field_id == FieldValue.id)
            .outerjoin(string_value1, string_value1.id == Value.id)
            .where(
                Issue.summary.like('(Integration Test Verification)%'),
                IssueCustomField.name == 'Stage',
                string_value1.value == 'Done'
            )
            .cte('last_completed')
        )

        parent_fix_versions = (
            select(
                child_alias.id_readable,
                icf.name,
                string_value2.value.label('fix_version')
            )
            .select_from(child_alias)
            .join(parent_alias, parent_alias.id_readable == child_alias.parent_id)
            .join(icf, parent_alias.id == icf.issue_id)
            .join(value, icf.value_id == value.field_id)
            .join(string_value2, value.id == string_value2.id)
            .where(icf.name == 'Fix versions')
        ).cte('parent_fix_versions')

        priorities = (
            select(
                child_alias.id_readable,
                string_value3.value.label('priority')
            ).select_from(child_alias
            ).join(icf, child_alias.id==icf.issue_id
            ).join(value, icf.value_id == value.field_id
            ).join(string_value3, string_value3.id == value.id
            ).where(icf.name == 'Priority')
        ).cte('priorities')

        stmt = (
            select(
                first_assignments.c.id_readable,
                first_assignments.c.timestamp.label('first_assigned_to_TCoE'),
                last_completed.c.completed_timestamp.label('last_set_as_done'),
                (last_completed.c.completed_timestamp - first_assignments.c.timestamp).label('time_difference'),
                parent_fix_versions.c.fix_version,
                priorities.c.priority
            ).join(
                last_completed,
                first_assignments.c.id_readable == last_completed.c.id_readable
            ).join(child_alias, first_assignments.c.id_readable == child_alias.id_readable
            ).join(parent_alias, child_alias.parent_id == parent_alias.id_readable
            ).join(parent_fix_versions, first_assignments.c.id_readable==parent_fix_versions.c.id_readable
            ).join(priorities, first_assignments.c.id_readable==priorities.c.id_readable
            ).where(
                first_assignments.c.rn == 1,
                last_completed.c.sn == 1
            )
            .order_by(first_assignments.c.id_readable)
        )

        with Session(engine) as session:
            with session.begin():
                result = session.execute(stmt).fetchall()

        response = []
        for id_readable,first_assigned_to_TCoE,last_set_as_done,time_difference,fix_version,priority in result:
            response.append({
                'id_readable':id_readable,
                #'first_assigned_to_TCoE':first_assigned_to_TCoE,
                #'last_set_as_done':last_set_as_done,
                'time_spent':time_difference,
                'fix_version':fix_version,
                'priority':priority
            })
        return response


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
    StringValue
)

from datetime import (
    datetime,
    timezone
)
from services.logger import logger

BATCH_SIZE = 1000

def convert_to_timestamp(date):
    return datetime.fromtimestamp(date/1000,tz=timezone.utc)

def extract_field_name(targetMember:str):

    match = re.search(r'__CUSTOM_FIELD__(\w+)_\d+', targetMember)

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
                for data in issue_data:
                    created=convert_to_timestamp(data.get('created'))
                    updated=convert_to_timestamp(data.get('updated'))
                    parent = data.get('parent')
                    parent_issues= parent.get('issues',None) if parent else None
                    
                    parent_issue_id = None
                    if parent_issues:
                        if isinstance(parent_issues, list) and parent_issues:
                            parent_issue_id = parent_issues[0].get('id', None)
                        elif isinstance(parent_issues, dict):
                            parent_issue_id = parent_issues.get('id', None)
                    
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
                            logger.info(f"abt to insert value {value}")
                            session.add(value)
                            session.commit()
                            session.refresh(value)
                            issueCustomField = IssueCustomField(
                                name=name,
                                value=value,
                                issue=issue
                            )
                            issue.custom_fields.append(issueCustomField)
                    
                    issues.append(issue)
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
                            logger.debug(f"Error while creating activity_item: {extract_field_name(targetMember)}, {issue_id_readable},{rm},{added},{data.get('timestamp')}")
                            raise
                    else:
                        logger.error(f"Custom field {extract_field_name(targetMember)} of issue {issue_id_readable} not found")
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
        icfv = aliased(IssueCustomFieldValue)

        type_exists = (
            select(1)
            .select_from(icf)
            .join(icfv, icf.id == icfv.custom_field_id)
            .where(
                icf.issue_id == Issue.id,
                icf.name == "Type",
                icfv.value_string == "Bug",
            )
            .exists()
        )

        origine_exists = (
            select(1)
            .select_from(icf)
            .join(icfv, icf.id == icfv.custom_field_id)
            .where(
                icf.issue_id == Issue.id,
                icf.name == "Origine",
                icfv.value_string == "Cliente"
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
        icfv = aliased(IssueCustomFieldValue)

        type_exists = (
            select(1)
            .select_from(icf)
            .join(icfv, icf.id == icfv.custom_field_id)
            .where(
                icf.issue_id == Issue.id,
                icf.name == "Type",
                icfv.value_string == "Bug",
            )
            .exists()
        )


        origine_exists = (
            select(1)
            .select_from(icf)
            .join(icfv, icf.id == icfv.custom_field_id)
            .where(
                icf.issue_id == Issue.id,
                icf.name == "Origine",
                icfv.value_string == "Cliente"
            )
            .exists()
        )

        issue_month = func.date_trunc("month", Issue.created)
        customer_bugs_stmt = (
            select(issue_month.label("month"),icfv.value_string,func.count().label("count"))
        ).select_from(Issue
        ).join(icf, Issue.id == icf.issue_id
        ).join(icfv, icf.id == icfv.custom_field_id
        ).where(type_exists,origine_exists,icf.name == "Product"
        ).group_by(func.date_trunc("month",Issue.created),icfv.value_string
        ).order_by(func.date_trunc("month",Issue.created))

        bugs_stmt = (
            select(issue_month.label("month"),icfv.value_string,func.count().label("count"))
        ).select_from(Issue
        ).join(icf, Issue.id == icf.issue_id
        ).join(icfv, icf.id == icfv.custom_field_id
        ).where(type_exists,icf.name == "Product"
        ).group_by(func.date_trunc("month",Issue.created),icfv.value_string
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
        logger.info(f"""
        customer_reported: {customer_reported_bugs}\n
        
        bugs: {bugs}\n

        customer_reported_by_product: {customer_reported_bugs_by_product}\n

        bugs_by_product: {bugs_by_product}\n
        """)

        for month,product,count in customer_reported_bugs_by_product:
            logger.info(f"Month pre elaboration: {month}")
            month= month.timestamp()*1000
            logger.info(f"Month post elaboration: {month}")
            product = product if product else 'Unknown'
            customer_bugs_dict_by_product[f"{month}{product}"] = count
        
        logger.info(f"dict: {customer_bugs_dict}\n dict_by_product:{customer_bugs_dict_by_product}")
        
        ratios = {}
        for month,total_count in bugs:
            month= month.timestamp()*1000
            if total_count > 10:
                customer_count = customer_bugs_dict.get(month,0)
                
                ratio = customer_count / total_count if total_count else 0
                
                if month not in ratios:
                    ratios[month]= {}
                ratios[month]['Total'] = ratio

        for month,product,count in bugs_by_product:
            month= month.timestamp()*1000
            if count> 10:
                product = product if product else 'Unknown'
                customer_count = customer_bugs_dict_by_product.get(f"{month}{product}",0)

                ratio = customer_count / count if total_count else 0

                if month not in ratios:
                    ratios[month] = {}

                ratios[month][product]= ratio
      
        results = [{'date': key, 'values': value} for key, value in ratios.items()]
        return results

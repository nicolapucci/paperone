from sqlalchemy import (
    select,
    exists,
    and_ ,
    func,
)
from sqlalchemy.orm import Session, aliased
from sqlalchemy.dialects.postgresql import insert


from services.postgres_engine import engine
from models.issues import (
    Issue,
    IssueCustomField,
    StringFieldValue,
    NumberFieldValue,
    DateFieldValue,
    IssueCustomFieldValue
)
from datetime import (
    datetime,
    timezone
)
from services.logger import logger

BATCH_SIZE = 1000

def convert_to_timestamp(date):
    return datetime.fromtimestamp(date/1000,tz=timezone.utc)

class IssueRepository:

    @staticmethod
    def bulk_create_issue_with_fields(issues_data:dict):#non in uso

        with Session(engine) as session:
            try:
                batch = []
                for issue_data in issues_data:
                    

                    issue = Issue(
                        youtrack_id=issue_data.get('id'),
                        id_readable=issue_data.get('idReadable'),
                        summary=issue_data.get('summary'),
                        created=convert_to_timestamp(issue_data.get('created')),
                        updated=convert_to_timestamp(issue_data.get('updated')),
                    )
                    batch.append(issue)


                    for field in issue_data.get('customFields', []):

                        name = field.get('name')
                        raw_value = field.get('value')
                        raw_value = raw_value.get('name') if isinstance(raw_value,dict) else raw_value
                        raw_value = raw_value if not isinstance(raw_value,list) else None
                        field_type = field.get('type', 'string')

                        custom_field = IssueCustomField(
                            issue=issue,
                            name=name
                        )

                        batch.append(custom_field)

                        if field_type == 'string':
                            custom_field_value = StringFieldValue(
                                field=custom_field
                            )
                            custom_field_value.value = raw_value

                        elif field_type == 'number':
                            custom_field_value = NumberFieldValue(
                                field=custom_field
                            )
                            custom_field_value.value = int(raw_value) if raw_value else None

                        elif field_type == 'date':
                            custom_field_value = DateFieldValue(
                                field=custom_field
                            )
                            custom_field_value.value = raw_value

                        else:
                            continue

                        batch.append(custom_field_value)
                        if len(batch)> BATCH_SIZE:
                            session.add_all(batch)
                            session.commit()
                            session.expunge_all()
                            batch = []
                if batch:
                    session.add_all(batch)
                    session.commit()
                    session.expunge_all()   

            except Exception:
                session.rollback()
                raise

    @staticmethod
    def get_max_updated_issue():
        stmt = select(func.max(Issue.updated))

        with Session(engine) as session:
            max_updated = session.execute(stmt).scalar_one_or_none()
        return max_updated.strftime('%Y-%m')

    @staticmethod
    def upsert_issues(issue_data:list):

        with Session(engine) as session:
            try:
                logger.info(f"Received {len(issue_data)} issues")
                issue_rows = []
                issueCustomField_rows = []
                issueCustomFieldValues_rows = []

                for data in issue_data:
                    created=convert_to_timestamp(data.get('created'))
                    updated=convert_to_timestamp(data.get('updated'))
                    issue = {
                        "youtrack_id":data.get('id'),
                        "id_readable":data.get('idReadable'),
                        "summary":data.get('summary'),
                        "created":created,
                        "updated":updated,
                    }
                    issue_rows.append(issue)
                    for field in data.get('customFields'):

                        name = field.get('name')
                        issueCustomField_rows.append({
                            "issue_id":data.get('id'),
                            "name":name,
                        })

                        value = field.get('value')
                        value = value if not isinstance(value,list) else value[0] if value else None#tmp incomplete fix
                        value = value if not isinstance(value,dict) else value.get('name')
                        
                        issueCustomFieldValues_rows.append({
                            "custom_field_id":f"{name}{data.get('id')}",#this is the same value as issueCustomField issue_id
                            "value_string":value,
                            "type":"string",
                        })


                logger.info(f"Upserting {len(issue_rows)} Issues...")
                stmt = (
                    insert(Issue
                    ).values(issue_rows
                    ).on_conflict_do_update(
                        index_elements=["youtrack_id"],
                        set_={
                            "summary":insert(Issue).excluded.summary,
                            "updated":insert(Issue).excluded.updated,
                        }
                    )
                    .returning(Issue.id, Issue.youtrack_id)
                )

                result = session.execute(stmt).fetchall()

                affetcted_rows = len(result)
                logger.info(f"returned {len(result)} rows")

                issue_id_map = {row.youtrack_id:row.id for row in result}
                issue_youtrack_id_map = {row.id:row.youtrack_id for row in result}

                for row in issueCustomField_rows:
                    row['issue_id'] = issue_id_map[row['issue_id']]

                logger.info(f"Upserting {len(issueCustomField_rows)} IssueCustomFields...")                
        
                stmt = (
                    insert(IssueCustomField
                    ).values(issueCustomField_rows
                    ).on_conflict_do_update(
                        index_elements=["issue_id","name"],
                        set_={
                            "name":insert(IssueCustomField).excluded.name
                        }
                    ).returning(
                        IssueCustomField.id,
                        IssueCustomField.name,
                        IssueCustomField.issue_id   ,
                    )
                )

                result = session.execute(stmt).fetchall()
                affetcted_rows += len(result)
                logger.info(f"returned {len(result)} rows")

                issueCustomField_id_map = {f"{row.name}{issue_youtrack_id_map[row.issue_id]}":row.id for row in result}#here i create the map with the same structure as the issuecustomFieldValue_rows' items
                
                for row in issueCustomFieldValues_rows:
                    row['custom_field_id'] = issueCustomField_id_map[row['custom_field_id']]
                
                logger.info(f"Upserting {len(issueCustomFieldValues_rows)} IssueCustomFieldValues...")
                stmt = (
                    insert(StringFieldValue
                    ).values(issueCustomFieldValues_rows
                    ).on_conflict_do_update(
                        index_elements=['custom_field_id'],
                        set_={
                            "value_string":insert(IssueCustomFieldValue).excluded.value_string
                        }
                    ).returning(
                        IssueCustomFieldValue.id,
                    )
                )

                result = session.execute(stmt).fetchall()
                affetcted_rows += len(result)
                logger.info(f"returned {len(result)} rows")

                session.commit()

                logger.info(f"Upsert completed, affected: {affetcted_rows} rows")



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
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
BATCH_SIZE = 1000

def convert_to_timestamp(date):
    return datetime.fromtimestamp(date/1000,tz=timezone.utc)

class IssueRepository:

    @staticmethod
    def bulk_create_issue_with_fields(issues_data:dict):

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
        return max_updated

    @staticmethod
    def update_issues(issue_data:list):#per ora evita i duplicati, ma deve aggiornare con i dati nuovi

        with Session(engine) as session:
            try:
                batch = []
                for issue_data in issues_data:
                    
                    issue = session.query(Issue).filter_by(id_readable= issue_data.get('idReadable')).first()

                    if not issue:

                        issue = Issue(
                            youtrack_id=issue_data.get('id'),
                            id_readable=issue_data.get('idReadable'),
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

    def defect_rate():
        (customer_reported_bugs,bugs) = IssueRepository.count_reported_bugs()
        customer_bugs_dict = {month: count for month,count in customer_reported_bugs}

        ratios = []

        for month,total_count in bugs:
            if total_count > 10:
                customer_count = customer_bugs_dict.get(month,0)
                
                ratio = customer_count / total_count if total_count else 0
                ratios.append({
                    "time": int(month.timestamp()*1000),
                    "ratio":ratio
                })


        return ratios
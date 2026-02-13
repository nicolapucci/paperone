from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import (
    select,
    func,
    and_
)

from services.postgres_engine import engine
from models.issues import (
    Issue,
    IssueCustomField,
    StringFieldValue,
    NumberFieldValue,
    DateFieldValue
)

BATCH_SIZE = 1000

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
                        created=issue_data.get('created'),
                        updated=issue_data.get('updated'),
                    )
                    batch.append(issue)


                    for field in issue_data.get('customFields', []):

                        name = field.get('name')
                        raw_value = field.get('value')
                        raw_value = raw_value.get('name') if isinstance(raw_value,dict) else raw_value
                        field_type = field.get('type', 'string')

                        custom_field = IssueCustomField(
                            issue=issue,
                            name=name
                        )

                        batch.append(custom_field)

                        if field_type == 'string':
                            custom_field_value = StringFieldValue(
                                custom_field=custom_field
                            )
                            custom_field_value.value = raw_value

                        elif field_type == 'number':
                            custom_field_value = NumberFieldValue(
                                custom_field=custom_field
                            )
                            custom_field_value.value = int(raw_value) if raw_value else None

                        elif field_type == 'date':
                            custom_field_value = DateFieldValue(
                                custom_field=custom_field
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

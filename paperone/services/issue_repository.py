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
    ActivityItem
)


class IssueRepository:

    @staticmethod
    def create_issue(yt_id, id_readable, origin, type, created):
        try:
            with Session(engine) as session:
                with session.begin():
                    issue = Issue(
                        youtrack_id=yt_id,
                        id_readable=id_readable,
                        origin=origin,
                        type=type,
                        created=created
                    )
                    session.add(issue)

                session.refresh(issue)  # opzionale, se vuoi id generato

            return issue

        except Exception as e:
            print(f"Error during issue creation!: {e}")
            return None


    @staticmethod
    def create_issue_bulk_raw(issue_dicts: list[dict]):
        try:
            with Session(engine) as session:
                stmt = insert(Issue).values(issue_dicts)
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=["youtrack_id"]
                )
                session.execute(stmt)
                session.commit()
        except Exception as e:
            print(f"Bulk insert failed: {e}")


    def user_reported_bugs():
        try:
            with Session(engine) as session:
                stmt = select(Issue).where(
                    and_(
                        Issue.type == 'Bug',
                        Issue.origin == 'Cliente'
                    )
                    )
                issues = session.execute(stmt)
                return issues
        except Exception as e:
            print(f"Error retrieving Issue data:  {e}")
            return None
    
    def user_reported_bugs_count():
        try:
            with Session(engine) as session:
                stmt = select(func.count()).select_from(Issue).where(
                    and_(
                        Issue.type == 'Bug',
                        Issue.origin == 'Cliente'
                    )
                )
                result = session.execute(stmt).scalar_one()
                return result
        except Exception as e:
            print(f"Error counting Issues: {e}")
            return 0
    
    def total_reported_bugs():
        try:
            with Session(engine) as session:
                stmt = select(func.count()).select_from(Issue).where(
                    Issue.type == 'Bug'
                )
                result = session.execute(stmt).scalar_one()
                return result
        except Exception as e:
            print(f"Error counting bugs: {e}")
            return 0
        



class ActivityItemRepository:

    @staticmethod
    def create_activityItem(removed,added,timestamp,issue_id,target_member):
        try:
            with Session(engine) as session:
                with session.begin():
                    activityItem = ActivityItem(
                        removed,
                        added,
                        timestamp,
                        issue_id,
                        target_member
                    )
                    session.add(activityItem)
                session.refresh()
                return activityItem
        except Exception as e:
            print(f"Error creating IssueActivity item: {e}")
            session.rollback()
            return None
        

    @staticmethod
    def bulk_create_actibityItem(activityItem_dicts:list[dict]):
        try:
            with Session(engine) as session:
                stmt = insert(ActivityItem).values(activityItem_dicts)
                session.execute(stmt)
                session.commit()
        except Exception as e:
            print(f"Error during bulk insert of ActivityItems: {e}")


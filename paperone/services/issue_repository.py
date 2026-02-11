from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from services.postgres_engine import engine
from models.issues import Issue


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

    def get_client_originated_bugs():
        try:
            with Session(engine) as session:
                
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
import csv


class TestRepository:

    CSV_MAP = {
    	"name": "Product name",
    	"version": "Version",
    	"id_readable": "Talking ID",
    	"automated": "Automatic",
    	"rc": "Rc version",
    	"outcome": "Test outcome",
	}

    def parse_row(row: dict):
        return {
            "product": {
            "name": row[CSV_MAP["name"]],
            "version": row[CSV_MAP["version"]],
            },
            "test": {
                "id_readable": row[CSV_MAP["id_readable"]],
                "automated": row[CSV_MAP["automated"]].strip().lower() in ("true", "1", "yes"),
            },
            "test_run": {
                "rc": int(row[CSV_MAP["rc"]]),
                "outcome": row[CSV_MAP["outcome"]],
            }
        }


    @staticmethod
    def import_tests_from_csv(filepath:str):
    
    with open("file.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        with Session(engine) as session:

            products_cache = {
                (p.name, p.version): p
                for p in session.query(Product).all()
            }

            tests_cache = {
                t.id_readable: t
                for t in session.query(Test).all()
            }

            for row in reader:
                data = parse_row(row)

                product_key = (
                    data["product"]["name"],
                    data["product"]["version"],
                )

                if product_key not in products_cache:
                    product = Product(**data["product"])
                    session.add(product)
                    session.flush()
                    products_cache[product_key] = product

                test_id = data["test"]["id_readable"]
                if test_id not in tests_cache:
                    test = Test(**data["test"])
                    session.add(test)
                    session.flush()
                    tests_cache[test_id] = test

                test_run = TestRun(
                    test=tests_cache[test_id],
                    release=products_cache[product_key],
                    **data["test_run"]
                )

                session.add(test_run)

            session.commit()

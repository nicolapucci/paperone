from sqlalchemy import (
    select,
    exists,
    and_ ,
    func,
)
from sqlalchemy.orm import Session, aliased
from sqlalchemy.dialects.postgresql import insert


from services.postgres_engine import engine

from models.tests import (
    Test,
    TestRun,
    Product
)
from services.logger import logger

from datetime import (
    datetime,
    timezone
)
import csv
import os

FTE = (3*40 + 2*32)*2

CSV_MAP = {
    	"name": "Product name",
    	"version": "Version",
    	"id_readable": "Talking ID",
    	"automated": "Automatic",
    	"rc": "Rc version",
    	"outcome": "Test outcome",
        "status": "Status",
	}

def parse_row(row: dict):
        return {
            "product": {
            "name": row[CSV_MAP["name"]],
            "version": row[CSV_MAP["version"]],
            },
            "test": {
                "id_readable": row[CSV_MAP["id_readable"]],
                "automated": row[CSV_MAP["automated"]].strip().lower() in ("ranorex automatic","full automatic"),
            },
            "test_run": {
                "rc": int(row[CSV_MAP["rc"]]),
                "outcome": row[CSV_MAP["outcome"]],
                "status": row[CSV_MAP["status"]]
            }
        }


class TestRepository:

    @staticmethod
    def import_tests_from_csv(filepath:str):#non in uso
    
        with open(filepath, newline="", encoding="utf-8-sig") as f:
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
                        stmt = insert(Product).values(
                            name=product.name,
                            version=product.version
                        ).on_conflict_do_nothing(
                            index_elements=["name","version"]
                        )
                        session.execute(stmt)

                        product = session.query(Product).filter_by(
                            name = product.name,
                            version = product.version
                        ).one()
                        products_cache[product_key] = product

                    test_id = data["test"]["id_readable"]
                    if test_id not in tests_cache:
                        test = Test(**data["test"])
                        stmt = insert(Test).values(
                            id_readable=test.id_readable,
                            automated = test.automated
                        ).on_conflict_do_nothing(
                            index_elements=["id_readable"]
                        )
                        session.execute(stmt)

                        test = session.query(Test).filter_by(
                            id_readable = test.id_readable
                        ).one()
                        tests_cache[test_id] = test

                    test_run = TestRun(
                        test=tests_cache[test_id],
                        release=products_cache[product_key],
                        **data["test_run"]
                    )

                    session.add(test_run)

                session.commit()

    @staticmethod
    def prepare_csv_for_import(folderpath:str):
        csv_dir = os.listdir(folderpath)
        result = []
        for filename in csv_dir:
            filepath = f"{folderpath}/{filename}"
            with open(filepath, newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    result.append(parse_row(row))
        return result

    @staticmethod
    def upsert_tests(test_data:list):

        with Session(engine) as session:
            try:
                logger.info(f"Received {len(test_data)} tests")
                product_rows = []
                test_rows = []
                testRun_rows = []

                for data in test_data:
                                        
                    name = data["product"]["name"]
                    version = data["product"]["version"]
                
                    id_readable = data["test"]["id_readable"]
                    automated = data['test']["automated"]

                    rc = data["test_run"]["rc"]
                    outcome= data["test_run"]["outcome"]
                    status= data["test_run"]["status"]

                    product = {
                        "name":name,
                        "version":version
                    }
                    if product not in product_rows:
                        product_rows.append(product)

                    test={
                        "id_readable":id_readable,
                        "automated":automated
                    }

                    existing_ids = {entry['id_readable'] for entry in test_rows}

                    if id_readable not in existing_ids:
                        test_rows.append(test)

                    unique_constraint = {f"{entry['test_id']}{entry['release_id']}{entry['rc']}" for entry in testRun_rows}
                    if f"{id_readable}{name}{version}{rc}" not in unique_constraint:
                        testRun_rows.append({
                            "test_id":id_readable,
                            "release_id":f"{name}{version}",
                            "rc":rc,    
                            "outcome":outcome,
                            "status":status
                        })
 
                stmt = (
                    insert(Product
                    ).values(product_rows
                    ).on_conflict_do_update(
                        index_elements=["name","version"],
                        set_={
                            "name":insert(Product).excluded.name
                        }
                    ).returning(Product.name,Product.version,Product.id)
                    )
                logger.info(f"Upserting {len(product_rows)} Products...")
                result = session.execute(stmt).fetchall()
                affetcted_rows = len(result)
                product_id_map = {f"{name}{version}":id for name,version,id in result}

                stmt = (
                    insert(Test
                    ).values(test_rows
                    ).on_conflict_do_update(
                        index_elements=["id_readable"],
                        set_={
                            "automated":insert(Test).excluded.automated
                        }
                    ).returning(Test.id_readable,Test.id)
                )
                logger.info(f"Upserting {len(test_rows)} Tests...")
                result = session.execute(stmt).fetchall()
                affetcted_rows += len(result)

                test_id_map = {id_readable:id for id_readable,id in result}

                for testRun in testRun_rows:
                    testRun["test_id"] = test_id_map[testRun["test_id"]] 
                    testRun["release_id"] = product_id_map[testRun["release_id"]]
                
                stmt = (
                    insert(TestRun
                    ).values(testRun_rows
                    ).on_conflict_do_update(
                        index_elements=["test_id","release_id","rc"],
                        set_={
                            "outcome":insert(TestRun).excluded.outcome,
                            "status":insert(TestRun).excluded.status,
                        }
                    ).returning(TestRun.id)
                )
                logger.info(f"Upserting {len(testRun_rows)} TestsRuns...")
                result = session.execute(stmt).fetchall()
                affetcted_rows += len(result)
                logger.info(f"Committing {affetcted_rows} changes...")
                session.commit()

            except Exception as e:
                logger.error(f"Error while upserting data: {e}")
                session.rollback()
                raise

           
    @staticmethod
    def test_over_fte():

        executed_tests_stmt = (
            select(Product.name,Product.version,func.count().label("count")
            ).select_from(Product
            ).join(TestRun, Product.id == TestRun.release_id
            ).join(Test, TestRun.test_id == Test.id
            ).where(TestRun.status != 'TO DO',TestRun.rc != 0
            ).group_by(Product.name,Product.version
            ).order_by(Product.version)
        )

        automated_tests_stmt = (
            select(Product.name,Product.version,func.count().label("count")
            ).select_from(Product
            ).join(TestRun, Product.id == TestRun.release_id
            ).join(Test, TestRun.test_id == Test.id
            ).where(Test.automated == True,TestRun.rc == 0
            ).group_by(Product.name,Product.version
            ).order_by(Product.version)
        )

        with Session(engine) as session:
            result = session.execute(stmt).fetchall()

        res = {}
        for name, version, count in result:
            if not version in res:
                res[version] = []
            res[version].append({
                'name':name,
                'count':count / FTE
                })
        return [{'version':version,'product':value['name'],'count':value['count']} for version,items in res.items() for value in items]

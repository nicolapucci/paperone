from services.test_repository import TestRepository

import json

try:

    TestRepository.import_tests_from_csv('./export_bugia.csv')
    

except Exception as e:
    print(f'Unable to seed the db: {e}')
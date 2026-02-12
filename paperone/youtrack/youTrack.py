import os
import requests

YOUTRACK_TOKEN = os.getenv('YOUTRACK_TOKEN')
YOUTRACK_URL = os.getenv('YOUTRACK_URL')

def bug_issues():

    fields = ''
    query= ''

    top = 1000
    skip = 0

    refetch = True

    issues = []

    while refetch:
        print('abt to fetch')

        response = requests.get(
            headers={
                "Content-Type":"application/json",
                "Accept":"application/json",
                "Authorization":f"Bearer {YOUTRACK_TOKEN}"
            },
            params={
                "fields":fields,
                "query":query,
                "$top":top,
                "$skip":skip
            },
            url= f"{YOUTRACK_URL}/api/issues"
        )

        issue_data = response.json()

        refetch = False if (skip%top == 3) else True
        #refetch = False if len(issue_data)<top else True
        
        skip += top
        issues.append(issue_data)


if __name__ == "__main__":
    print(bug_issues())
import os
import requests

YOUTRACK_TOKEN = os.getenv('YOUTRACK_TOKEN')
YOUTRACK_URL = os.getenv('YOUTRACK_URL')


fields = 'id,idReadable,created,customFields(name,value(name))'
base_query= 'project; Kalliope Type: Bug'

last_update = None

update_query = f"{base_query} updated: {last_update} .. Now" #to check the type of the timestamp accepted by youtrack


def issues(fields,query):

    top = 1000
    skip = 0

    refetch = True

    issues = []

    while refetch:

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

        refetch = False if len(issue_data)<top else True #if youtrack returns less items than requested it means he don-t have any more items to return
        
        skip += top
        newdata = []
        for issue in issue_data:
            newitem = issue.copy()
            for field in newdata.customFields:
                name = field['name']
                value = field['value']['name'] if (value in field and name in field['value']) else None
                if(name in ['Origine', 'Type']):
                    newitem[name]=value
            del newitem.customFields
            newdata.append(newitem)

        issues.append(newdata)
    return issues

if __name__ == "__main__":
    print(issues())
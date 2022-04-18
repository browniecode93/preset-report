import urllib3
import collections
import json
import csv
from datetime import datetime

http = urllib3.PoolManager()
report = {}
def authorize(http):
    encoded_data = json.dumps({'name': 'preset_secret_name',
                               'secret': 'preset_token_secret'})
    resp = http.request(
        "POST",
        "https://manage.app.preset.io/api/v1/auth/",
        retries=urllib3.util.Retry(3),
        headers={'Content-Type': 'application/json'},
        body=encoded_data
    )

    data = json.loads(resp.data)
    auth_token = data["payload"]["access_token"]
    print('auth_token has been cerated!')
    return auth_token

def check_if_in_list_of_dict(dict, value, ent):
    """Check if given key exists in list of dictionaries """
    if value in dict:
        if ent in dict[value]:
            return True
    return False


def create_preset_reports(http, auth_token):
    header = {'Authorization': 'Bearer ' + auth_token}
    page_size = 50
    page_num = 0
    user_dataset = collections.defaultdict(dict)
    entities = ['chart', 'dashboard', 'dataset']

    for ent in entities:
        url = f"https://{dashboard_id}.us1a.app.preset.io/api/v1/{ent}/?q=(order_column:changed_on_delta_humanized,order_direction:desc,page:{page_num},page_size:{page_size})"
        print(url)
        while(1):
            resp = http.request(
                "GET",
                url,
                retries=urllib3.util.Retry(3),
                headers=header
            )
            data = json.loads(resp.data)
            if(not data['result']):
            #if(page_num == 1):
                break

            results = data['result']

            for res in results:
                for owner in res['owners']:
                    full_name = f"{owner['first_name']} {owner['last_name']}"
                    if check_if_in_list_of_dict(user_dataset, full_name, ent):
                        user_dataset[full_name][ent] += 1
                    else:
                        user_dataset[full_name][ent] = 1

            page_num += 1
            url = f"https://{dashboard_id}.us1a.app.preset.io/api/v1/{ent}/?q=(order_column:changed_on_delta_humanized,order_direction:desc,page:{page_num},page_size:{page_size})"
        page_num = 0

    return user_dataset

def write_to_csv(user_dataset):
    row = []
    with open('preset-user.csv', 'w') as ofile:
        wr = csv.writer(ofile)
        wr.writerow(['user','dataset_count','dashboard_count','chart_count', 'data_lake_count', 'factorial_s3_count', 'updated_at'])
        entities = ['dataset', 'dashboard', 'chart', 'data_lake_count', 'factorial_s3_count']
        for user in user_dataset:
            row.append(user)
            for ent in entities:
                if check_if_in_list_of_dict(user_dataset, user, ent):
                    row.append(user_dataset[user][ent])
                else:
                    row.append(0)
            row.append(datetime.today().strftime('%Y-%m-%d'))
            wr.writerow(row)
            row = []


token = authorize(http)
report = create_preset_reports(http, token)
print(report)
write_to_csv(report)



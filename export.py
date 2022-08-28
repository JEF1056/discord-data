import json
import dataset

db_url = "postgresql://admin:r3wyhgifqloprm9u0iqh4t89qu2evrq2gie8rd9uj23rlhf8q3uyghfvcjm2pw@localhost:5432/postgres"

db = dataset.connect(db_url)

with open('dataset.json', "w") as f:
    for line in db['dataset'].all():
        f.write(json.dumps(dict(line))+"\n")

with open('dataset_nopersona.json', "w") as f:
    for line in db['dataset_nopersona'].all():
        f.write(json.dumps(dict(line))+"\n")
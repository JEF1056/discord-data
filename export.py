import json
import random
import dataset
from tqdm import tqdm

db_url = "postgresql://admin:r3wyhgifqloprm9u0iqh4t89qu2evrq2gie8rd9uj23rlhf8q3uyghfvcjm2pw@localhost:5432/postgres"

db = dataset.connect(db_url)

with open('dataset_train.json', "w") as train, open('dataset_validation.json', "w") as valid:
    ds = db['dataset']
    for line in tqdm(ds.all(), total=ds.count(), desc="dataset"):
        random.choices([train, valid], weights=[0.9, 0.1])[0].write(json.dumps(dict(line))+"\n")

with open('dataset_nopersona_train.json', "w") as train, open('dataset_nopersona_validation.json', "w") as valid:
    ds = db['dataset_nopersona']
    for line in tqdm(ds.all(), total=ds.count(), desc="dataset_nopersona"):
        random.choices([train, valid], weights=[0.9, 0.1])[0].write(json.dumps(dict(line))+"\n")
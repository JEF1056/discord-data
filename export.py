import json
import random
import dataset
from tqdm import tqdm

db_url = "postgresql://admin:r3wyhgifqloprm9u0iqh4t89qu2evrq2gie8rd9uj23rlhf8q3uyghfvcjm2pw@localhost:5432/postgres"

db = dataset.connect(db_url)

for dataset in ["dataset", "dataset_nopersona"]:
    with open(f'{dataset}_train.json', "w") as train, open(f'{dataset}_validation.json', "w") as valid:
        ds = db[dataset]
        distinct_questions = ds.distinct("ref_id", "answer")
        for question in [ref_id["ref_id"] for ref_id in ds.distinct("ref_id")]:
            result = ds.query("SELECT DISTINCT ref_id, ")
            for line in tqdm(ds.all(), total=ds.count(), desc="dataset"):
                random.choices([train, valid], weights=[0.9, 0.1])[0].write(json.dumps(dict(line))+"\n")
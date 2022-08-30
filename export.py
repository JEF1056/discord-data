import json
import random
import dataset
from tqdm import tqdm
import os

db_url = "postgresql://admin:r3wyhgifqloprm9u0iqh4t89qu2evrq2gie8rd9uj23rlhf8q3uyghfvcjm2pw@localhost:5432/postgres"

db = dataset.connect(db_url)

version = "v0.1"

counter = 0

for dataset in ["dataset", "dataset_nopersona"]:
    with open(f"{dataset}_train.json", "w") as train, open(f"{dataset}_validation.json", "w") as valid:
        train.write(json.dumps({
            "version": version,
            "split": "train",
            "data": []
        })[:-2])

        valid.write(json.dumps({
            "version": version,
            "split": "validation",
            "data": []
        })[:-2])

        distinct_entries = {}
        for entry in db.query(f"SELECT DISTINCT * FROM {dataset}"):
            if entry["ref_id"] in distinct_entries:
                distinct_entries[entry["ref_id"]].append(entry)
            else:
                distinct_entries[entry["ref_id"]] = [entry]

        for question in tqdm(distinct_entries):
            split = random.choices([train, valid], weights=[0.9, 0.1])[0]
            for message in distinct_entries[question]:
                obj = dict(message)
                obj["question_id"]=obj["ref_id"]
                obj["answer_id"]=obj["id"]
                obj["id"]=counter
                obj["answers"] = {"text":[obj["answer"]]}
                del obj["answer"]
                del obj["ref_id"]
                split.write(json.dumps(obj)+",")
                counter+=1
    
    with open(f"{dataset}_train.json", "rb+") as train, open(f"{dataset}_validation.json", "rb+") as valid:
        for closer in [train, valid]:
            closer.seek(-1, os.SEEK_END)
            closer.truncate()
    
    with open(f"{dataset}_train.json", "a+") as train, open(f"{dataset}_validation.json", "a+") as valid:
        for closer in [train, valid]:
            closer.write("]}")

    print(counter)
import json
import random
import dataset
from tqdm import tqdm

db_url = "postgresql://admin:r3wyhgifqloprm9u0iqh4t89qu2evrq2gie8rd9uj23rlhf8q3uyghfvcjm2pw@localhost:5432/postgres"

db = dataset.connect(db_url)

version = "v0.1"

counter = 0

for dataset in ["dataset", "dataset_nopersona"]:
    train = {
        "version": version,
        "split": "train",
        "data": []
    }

    valid = {
        "version": version,
        "split": "validation",
        "data": []
    }

    ds = db[dataset]
    try:
        distinct_questions = list(ds.distinct("ref_id", "persona", "question"))
    except:
        distinct_questions = list(ds.distinct("ref_id", "question"))
    distinct_answers = {}
    for answer in ds.distinct("ref_id", "answer"):
        if answer["ref_id"] in distinct_answers:
            distinct_answers[answer["ref_id"]].append(answer["answer"])
        else:
            distinct_answers[answer["ref_id"]] = [answer["answer"]]

    for question in tqdm(distinct_questions, desc=dataset):
        line = dict(question)
        line["id"] = counter
        line["answers"]=dict(text=distinct_answers[question["ref_id"]])

        random.choices([train, valid], weights=[0.9, 0.1])[0]["data"].append(line)
        counter += 1

    json.dump(train, open(f"{dataset}_train.json", "w"))
    json.dump(valid, open(f"{dataset}_validation.json", "w"))
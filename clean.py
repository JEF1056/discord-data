import os
import math
import ijson
import dataset
from tqdm import tqdm
import multiprocessing
import concurrent.futures
from src.helpers import clean

max_length = 512
data_folder = "data"
cache_folder = "cleaned"
processing_cores = math.ceil(multiprocessing.cpu_count() * 0.6875)
print(f"{processing_cores} cores for workers, {multiprocessing.cpu_count()-processing_cores} cores for db")

db_url = "postgresql://admin:r3wyhgifqloprm9u0iqh4t89qu2evrq2gie8rd9uj23rlhf8q3uyghfvcjm2pw@localhost:5432/postgres"

def table_ok(table):
    with dataset.connect(db_url) as db:
        status = db['status'].find_one(ref=table)
        if status and dict(status)["ok"]:
            return True
    return False

def chunk(arr, arr_len, n):
    arr = list(arr)
    chunk_size = math.ceil(arr_len/n)
    return (arr[i:i+chunk_size] for i in range(0, arr_len, chunk_size)), math.ceil(arr_len/chunk_size)

def cache_users(path):
    db = dataset.connect(db_url)

    messages_processed = 0
    names = []
    personas = []

    with open(os.path.join(data_folder, "intros", path), "r") as f:
        messages = ijson.items(f, 'messages.item')
        for message in messages:
            # Ignore if content is:
            # - too short
            # - has an embed
            # - is a non-default message
            # - author is a bot
            # - author's name is too long
            if (len(message["content"]) < 100 or
                len(message["content"]) > max_length*0.95 or
                message["embeds"] != [] or
                message["type"] != "Default" or
                message["author"]["isBot"] == True or
                message["author"]["name"] == "Deleted User" or 
                len(message["author"]["name"]) > 100):
                continue

            cleaned_name = clean(message["author"]["name"])
            cleaned_message = clean(message["content"])

            if not (cleaned_message and cleaned_name):
                continue

            # Process in memory to increase speed
            if len(cleaned_name) < 100:
                names.append(dict(
                    id=message['author']['id'],
                    name=cleaned_name
                ))

            if len(cleaned_message) < max_length * 0.95:
                personas.append(dict(
                    id=message['author']['id'],
                    persona=cleaned_message
                ))

            messages_processed+=1
        
        # Insert in bulk
        db["names_temp"].insert_many(names, chunk_size=50_000)
        db["personas_temp"].insert_many(personas, chunk_size=50_000)

    db.close()

    return messages_processed

def cache_messages(path):
    db = dataset.connect(db_url)

    messages_processed = 0
    names = []
    msgs = []

    with open(os.path.join(data_folder, "content", path), "r") as f:
        messages = ijson.items(f, 'messages.item')
        for message in messages:
            # Ignore if content is:
            # - too long
            # - has an embed
            # - is a non-default/reply message
            # - author is a bot
            # - author is a "Deleted User"
            # - author's name is too long
            if (len(message["content"]) > max_length*0.95 or
                len(message["content"]) <= 1 or
                message["embeds"] != [] or
                message["type"] not in ["Default", "Reply"] or
                message["author"]["isBot"] == True or
                message["author"]["name"] == "Deleted User" or
                len(message["author"]["name"]) > 100):
                continue

            cleaned_name = clean(message["author"]["name"])
            cleaned_message = clean(message["content"])

            if not (cleaned_message and cleaned_name):
                continue

            # Process in memory to increase speed
            if len(cleaned_name) < 100:
                names.append(dict(
                    id=message['author']['id'],
                    name=cleaned_name
                ))

            if len(cleaned_message) < max_length * 0.95:
                if message["type"] == "Reply":
                    msgs.append(dict(
                        id=message['id'],
                        author_id=message['author']['id'],
                        content=cleaned_message,
                        refs=message["reference"]["messageId"]
                    ))
                else:
                    msgs.append(dict(
                        id=message['id'],
                        author_id=message['author']['id'],
                        content=cleaned_message
                    ))

            messages_processed+=1

        # Insert in bulk
        db["names_temp"].insert_many(names, chunk_size=100_000)
        db["messages_temp"].insert_many(msgs, chunk_size=100_000)

    db.close()

    return messages_processed

def create_dataset(c):
    db = dataset.connect(db_url)

    messages_processed = 0
    pairs = []
    pairs_nopersona = []

    # There is only one set of messages and one set of references.
    # However, there can be multiple personas and author names per yuser, and we must account for that
    messages = list(c)
    references = {i["id"]:{"name": i["author_id"], "content": i["content"]} for i in db["messages"].find(id={"in": [message["refs"] for message in messages]})}
    personas = {}
    reference_author_names = {}

    # Generate this thread's persona cache
    for persona in db["personas"].find(id={"in": [message["author_id"] for message in messages]}):
        if persona["id"] in personas:
            personas[persona["id"]].append(persona["persona"])
        else:
            personas[persona["id"]] = [persona["persona"]]

    # Generate this thread's references cache
    for name in db["names"].find(id={"in":[references[message]["name"] for message in references]}):
        if name["id"] in reference_author_names:
            reference_author_names[name["id"]].append(name["name"])
        else:
            reference_author_names[name["id"]] = [name["name"]]

    for message in messages:
        messages_processed += 1

        try:
            context = references[message["refs"]]
            # names = reference_author_names[context["name"]]
        except KeyError:  continue

        try:
            persona = personas[message["author_id"]]
            for p in persona:
                # for name in names:
                #     pairs.append(dict(
                #         id=message['id'],
                #         ref_id=message["refs"],
                #         persona = p,
                #         question = f"{name}: {context['content']}",
                #         answer = message['content']
                #     ))
                pairs.append(dict(
                    id=message['id'],
                    ref_id=message["refs"],
                    persona = p,
                    question = context['content'],
                    answer = message['content']
                ))
        except KeyError: pass

        # for name in names:
        #     pairs_nopersona.append(dict(
        #         id=message['id'],
        #         ref_id=message["refs"],
        #         question = f"{name}: {context['content']}",
        #         answer = message['content']
        #     ))
        pairs_nopersona.append(dict(
            id=message['id'],
            ref_id=message["refs"],
            question = context['content'],
            answer = message['content']
        ))

    # Insert in bulk
    db["dataset"].insert_many(pairs, chunk_size=100_000)
    db["dataset_nopersona"].insert_many(pairs_nopersona, chunk_size=100_000)

    db.close()

    return messages_processed

if __name__ == "__main__":
    db = dataset.connect(db_url)

    # Create status table, clear dataset
    db.query(f"""
    DROP TABLE IF EXISTS dataset;
    DROP TABLE IF EXISTS dataset_nopersona;

    CREATE TABLE dataset (
        id BIGINT NOT NULL,
        ref_id BIGINT NOT NULL,
        persona VARCHAR({max_length}) NOT NULL,
        question VARCHAR({max_length}) NOT NULL,
        answer VARCHAR({max_length}) NOT NULL
    );

    CREATE TABLE dataset_nopersona (
        id BIGINT NOT NULL,
        ref_id BIGINT NOT NULL,
        question VARCHAR({max_length}) NOT NULL,
        answer VARCHAR({max_length}) NOT NULL
    );

    CREATE TABLE IF NOT EXISTS status (
        ref VARCHAR(100) PRIMARY KEY,
        ok BOOL
    )
    """)

    status = db['status']

    # Process and cache data
    if not (table_ok("names") and table_ok("personas")):
        status.upsert_many([
            dict(
                ref = "personas",
                ok = False
            ),
            dict(
                ref = "names",
                ok = False
            )
        ], ["ref"])

        db.query(f"""
        DROP TABLE IF EXISTS personas_temp;
        DROP TABLE IF EXISTS personas;
        DROP TABLE IF EXISTS names_temp;
        ALTER TABLE IF EXISTS names RENAME TO names_temp;

        CREATE TABLE personas_temp (
            id BIGINT NOT NULL,
            persona VARCHAR({max_length}) NOT NULL
        );

        CREATE TABLE IF NOT EXISTS names_temp (
            id BIGINT NOT NULL,
            name VARCHAR(100) NOT NULL
        );
        """)

        # One-pass, no constraint processing of intro data
        tasks = os.listdir(os.path.join(data_folder, "intros"))
        with concurrent.futures.ProcessPoolExecutor(max_workers=processing_cores) as executor:
            total = list(tqdm(executor.map(cache_users, tasks), total=len(tasks), desc="Caching users..."))
        
        # Let sql handle uniqueness processing
        db.query("SELECT DISTINCT * INTO personas FROM personas_temp;")
        db.query("DROP TABLE personas_temp;")
        db.query("SELECT DISTINCT * INTO names FROM names_temp;")
        db.query("DROP TABLE names_temp;")

        # Print some stats about the work that just happened
        print(f"""
        personas: {db["personas"].count()}
        names: {db["names"].count()}
        messages processed: {sum(total)}
        """)

        status.upsert_many([
            dict(
                ref = "personas",
                ok = True
            ),
            dict(
                ref = "names",
                ok = True
            )
        ], ["ref"])
    else:
        print("Persona data OK. Skipping.")

    if not table_ok("messages"):
        status.upsert(dict(
            ref = "messages",
            ok = False
        ), ["ref"])

        db.query(f"""
        DROP TABLE IF EXISTS messages_temp;
        DROP TABLE IF EXISTS messages;
        DROP TABLE IF EXISTS names_temp;
        ALTER TABLE IF EXISTS names RENAME TO names_temp;

        CREATE TABLE messages_temp (
            id BIGINT NOT NULL,
            author_id BIGINT NOT NULL,
            refs BIGINT,
            content VARCHAR({max_length}) NOT NULL
        );

        CREATE TABLE IF NOT EXISTS names_temp (
            id BIGINT NOT NULL,
            name VARCHAR(100) NOT NULL
        );
        """)

        # One-pass, no constraint processing of message data
        tasks = os.listdir(os.path.join(data_folder, "content"))
        # for t in tqdm(tasks):
        #     cache_messages(t)
        with concurrent.futures.ProcessPoolExecutor(max_workers=processing_cores) as executor:
            total = list(tqdm(executor.map(cache_messages, tasks), total=len(tasks), desc="Caching messages..."))
        
        # Let sql handle uniqueness processing
        db.query("SELECT DISTINCT * INTO names FROM names_temp;")
        db.query("DROP TABLE names_temp;")
        db.query("SELECT DISTINCT * INTO messages FROM messages_temp;")
        db.query("DROP TABLE messages_temp")

        # Print some stats about the work that just happened
        print(f"""
        messages: {db["messages"].count()}
        names: {db["names"].count()}
        messages processed: {sum(total)}
        """)

        status.upsert(dict(
            ref = "messages",
            ok = True
        ), ["ref"])
    else:
        print("Message and Name data OK. Skipping.")

    messages = db["messages"]

    tasks, total_tasks = chunk(messages.find(refs={"not": None}),  messages.count(refs={"not": None}), processing_cores*24)
    with concurrent.futures.ProcessPoolExecutor(max_workers=processing_cores) as executor:
        total = list(tqdm(executor.map(create_dataset, tasks), total=total_tasks, desc="Creating dataset..."))

    print(f"""
    with persona: {db["dataset"].count()}
    no persona: {db["dataset_nopersona"].count()}
    expected: {messages.count(refs={"not": None})}
    processed: {sum(total)}
    """)
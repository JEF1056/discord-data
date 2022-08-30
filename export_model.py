import os
import re
import argparse
os.environ["CUDA_VISIBLE_DEVICES"] = "-1"
from transformers import AutoTokenizer
from src.model import CustomTFT5
import shutil

parser = argparse.ArgumentParser(description='Clean Discord data')
parser.add_argument('-model', type=str, default="tst-jade",
                    help='model name or model dir')
args = parser.parse_args()

tokenizer = AutoTokenizer.from_pretrained("t5-small")
tokenizer.save_pretrained("export/tokenizer", saved_model=True)
max_len = tokenizer.model_max_length if tokenizer.model_max_length < 2048 else 2048

model_folder = None
if os.path.isdir(args.model):
    if "pytorch_model.bin" not in os.listdir(args.model):
        start_step = 0
        for checkpoint in [f for f in os.listdir(args.model) if f.startswith("checkpoint-")]:
            checkpoint_nums = int(re.findall(r"checkpoint-(\d+)", checkpoint)[0])
            if start_step < checkpoint_nums: start_step = checkpoint_nums
        model_folder = f"checkpoint-{start_step}"
    else:
        model_folder = ""
    print(model_folder)
    assert model_folder != None, "Cannot find model"
    selected = os.path.join(args.model, model_folder)
    print(f"LOADING FROM CHECKPOINT {selected}")
    model = CustomTFT5.from_pretrained(selected, from_pt=True)

if model_folder == None:
    print(f"STARTING NEW INSTANCE")
    model = CustomTFT5.from_pretrained(args.model)
# model.resize_token_embeddings(len(tokenizer))

while True:
    inp = input("> ")
    if inp == "exit": break
    inputs = tokenizer(f"question: {inp} persona: The names Zeke (online name) o Zarain, Im from Philippines, i like to watch anime, play games, explore random stuff and get new friends, hope we get along and hopefully find friends and maybe best friends too-", return_tensors="tf")
    outputs = model.generate(inputs.input_ids, max_new_tokens=64)
    print(outputs)
    print(tokenizer.decode(outputs[0]))

if os.path.exists("test"):
    shutil.rmtree("test")

model.save_pretrained("export/model", saved_model=True)
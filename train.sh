#!/usr/bin/env bash

# args tuned for RTX 2060 6gb, Ryzen 7 4800H
python3 train.py \
    --model_name_or_path t5-small \
    --do_train \
    --do_eval \
    --train_file dataset_train_nopersona.json \
    --validation_file dataset_validation_nopersona.json \
    --output_dir tst-model \
    --per_device_train_batch_size=10 \
    --per_device_eval_batch_size=10 \
    --learning_rate 3e-5 \
    --num_train_epochs 2 \
    --max_seq_length 512 \
    --max_answer_length 128 \
    --predict_with_generate \
    --question_column question \
    --answer_column answers \
    --preprocessing_num_workers 14 \
    --save_total_limit 3 \
    --save_steps 2500 \
    --logging_steps 500 \
    --half_precision_backend apex \
    --fp16 True
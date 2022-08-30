from transformers import TFT5ForConditionalGeneration, AutoTokenizer
import tensorflow as tf

tokenizer = AutoTokenizer.from_pretrained("t5-small")
max_len = tokenizer.model_max_length if tokenizer.model_max_length < 2048 else 2048

class CustomTFT5(TFT5ForConditionalGeneration):
    def __init__(self, *args, log_dir=None, cache_dir=None, **kwargs):
        super().__init__(*args, **kwargs)

    @tf.function(input_signature=[{
        "input_ids": tf.TensorSpec([None, max_len], tf.int32, name="input_ids"),
        "attention_mask": tf.TensorSpec([None, max_len], tf.int32, name="attention_mask"),
        "decoder_input_ids": tf.TensorSpec([None, max_len], tf.int32, name="decoder_input_ids"),
        "decoder_attention_mask": tf.TensorSpec([None, max_len], tf.int32, name="decoder_attention_mask"),
    }])
    def serving(self, inputs):
        output = self.call(inputs, use_cache=True)
        return self.serving_output(output)

class SaveCallback(tf.keras.callbacks.Callback):
    def __init__(self, model, path):
        super().__init__()
        self.model = model
        self.path = path

    def on_train_end(self, logs=None):
        logs.update({"epoch":"Final"})
        self.model.save_pretrained(self.path.format(**logs))

    def on_epoch_end(self, epoch, logs=None):
        logs.update({"epoch":epoch})
        self.model.save_pretrained(self.path.format(**logs))
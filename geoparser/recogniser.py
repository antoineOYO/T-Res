import os
import sys
from functools import partial
from pathlib import Path

import numpy as np
from datasets import load_dataset, load_metric
from transformers import (
    AutoModelForTokenClassification,
    AutoTokenizer,
    DataCollatorForTokenClassification,
    Trainer,
    TrainingArguments,
    pipeline,
)

# Add "../" to path to import utils
sys.path.insert(0, os.path.abspath(os.path.pardir))
from utils import ner


class Recogniser:
    def __init__(
        self,
        model,
        train_dataset="",
        test_dataset="",
        pipe=None,
        base_model="",
        model_path="",
        training_args=dict(),
        overwrite_training=False,
        do_test=False,
        load_from_hub=False,
    ):
        """
        Initialises a Recogniser object.

        Arguments:
            model (str): The name of the NER model.
            train_dataset (str): Path to the dataset used for training.
            test_dataset (str): Path to the dataset used for testing.
            pipe (None): We'll store the NER pipeline here.
            base_model (str): Path to base model to fine-tune
            model_path (str): Path to output folder where the model will be stored.
            training_args (dict): Dictionary of fine-tuning args.
            overwrite_training (bool): True to overwrite training, False otherwise.
            do_test (bool): True to run it on test mode, False otherwise.
            load_from_hub (bool): True if the model is in the Huggingface hub.
        """
        self.model = model
        self.train_dataset = train_dataset
        self.test_dataset = test_dataset
        self.pipe = pipe
        self.base_model = base_model
        self.model_path = model_path
        self.training_args = training_args
        self.overwrite_training = overwrite_training
        self.do_test = do_test
        self.load_from_hub = load_from_hub

        # Add "_test" to the model name if do_test is True, unless
        # the model is downloaded from Huggingface, in which case
        # we keep the name inputed by the user.
        if self.do_test == True and self.load_from_hub == False:
            self.model += "_test"

    # -------------------------------------------------------------
    def __str__(self):
        """
        Print the string representation of the Recogniser object.
        """
        s = (
            "\n>>> Toponym recogniser:\n"
            "    * Model path: {0}\n"
            "    * Model name: {1}\n"
            "    * Base model: {2}\n"
            "    * Overwrite model if exists: {3}\n"
            "    * Train in test mode: {4}\n"
            "    * Load from hub: {5}\n"
            "    * Training args: {6}\n"
        ).format(
            self.model_path,
            self.model,
            self.base_model,
            str(self.overwrite_training),
            str(self.do_test),
            str(self.load_from_hub),
            str(self.training_args),
        )
        return s

    # -------------------------------------------------------------
    def train(self):
        """
        Train a NER model. The training will be skipped if the model already
        exists and self.overwrite_training it set to False, or if the NER model
        is obtained from HuggingFace. The training will be run on test mode if
        self.do_test is set to True.

        Returns:
            A trained NER model.

        Notes:
            Credit: This function is adapted from a HuggingFace tutorial:
            https://github.com/huggingface/notebooks/blob/master/examples/token_classification.ipynb.
        """

        # Skip training if the model is obtained from the hub:
        if self.load_from_hub == True:
            return None

        # If model exists and overwrite is set to False, skip training:
        if (
            Path(self.model_path + self.model + ".model").exists()
            and self.overwrite_training == False
        ):
            print(
                "\n** Note: Model "
                + self.model_path
                + self.model
                + ".model is already trained. Set overwrite to True if needed.\n"
            )
            return None

        print("*** Training the toponym recognition model...")

        # Create a path to store the model if it does not exist:
        Path(self.model_path).mkdir(parents=True, exist_ok=True)

        # Use the "seqeval" metric to evaluate the predictions during training:
        metric = load_metric("seqeval")

        # Load train and test sets:
        # Note: From https://huggingface.co/docs/datasets/loading: "A dataset
        # without a loading script by default loads all the data into the train
        # split."
        if self.do_test == True:
            # If test is True, train on a portion of the train and test sets:
            lwm_train = load_dataset(
                "json", data_files=self.train_dataset, split="train[:10]"
            )
            lwm_test = load_dataset(
                "json", data_files=self.test_dataset, split="train[:10]"
            )
        else:
            lwm_train = load_dataset(
                "json", data_files=self.train_dataset, split="train"
            )
            lwm_test = load_dataset("json", data_files=self.test_dataset, split="train")

        print("Train:", len(lwm_train))
        print("Test:", len(lwm_test))

        # Obtain unique list of labels:
        df_tmp = lwm_train.to_pandas()
        label_list = sorted(list(set([x for l in df_tmp["ner_tags"] for x in l])))

        # Create mapping between labels and ids:
        id2label = dict()
        for i in range(len(label_list)):
            id2label[i] = label_list[i]
        label2id = {v: k for k, v in id2label.items()}

        # Load model and tokenizer:
        model = AutoModelForTokenClassification.from_pretrained(
            self.base_model,
            num_labels=len(label_list),
            id2label=id2label,
            label2id=label2id,
        )
        tokenizer = AutoTokenizer.from_pretrained(self.base_model)
        data_collator = DataCollatorForTokenClassification(tokenizer)

        # Align tokens and labels when training:
        lwm_train_tok = lwm_train.map(
            partial(
                ner.training_tokenize_and_align_labels,
                tokenizer=tokenizer,
                label_encoding_dict=label2id,
            ),
            batched=True,
        )
        lwm_test_tok = lwm_test.map(
            partial(
                ner.training_tokenize_and_align_labels,
                tokenizer=tokenizer,
                label_encoding_dict=label2id,
            ),
            batched=True,
        )

        # Compute metrics when training:
        def compute_metrics(p):
            predictions, labels = p
            predictions = np.argmax(predictions, axis=2)

            # Remove ignored index (special tokens)
            true_predictions = [
                [label_list[p] for (p, l) in zip(prediction, label) if l != -100]
                for prediction, label in zip(predictions, labels)
            ]
            true_labels = [
                [label_list[l] for (p, l) in zip(prediction, label) if l != -100]
                for prediction, label in zip(predictions, labels)
            ]

            results = metric.compute(
                predictions=true_predictions, references=true_labels
            )
            return {
                "precision": results["overall_precision"],
                "recall": results["overall_recall"],
                "f1": results["overall_f1"],
                "accuracy": results["overall_accuracy"],
            }

        training_args = TrainingArguments(
            output_dir=self.model_path,
            evaluation_strategy="epoch",
            logging_dir=self.model_path + "runs/" + self.model,
            learning_rate=self.training_args["learning_rate"],
            per_device_train_batch_size=self.training_args["batch_size"],
            per_device_eval_batch_size=self.training_args["batch_size"],
            num_train_epochs=self.training_args["num_train_epochs"],
            weight_decay=self.training_args["weight_decay"],
        )

        trainer = Trainer(
            model=model,
            args=training_args,
            train_dataset=lwm_train_tok,
            eval_dataset=lwm_test_tok,
            data_collator=data_collator,
            tokenizer=tokenizer,
            compute_metrics=compute_metrics,
        )

        # Train the model:
        trainer.train()

        # Evaluate the training:
        trainer.evaluate()

        # Save the model:
        trainer.save_model(self.model_path + self.model + ".model")

    # -------------------------------------------------------------
    def create_pipeline(self):
        """
        Create a pipeline for performing NER given a NER model.

        Returns:
            self.model (str): the model name.
            self.pipe (Pipeline): a pipeline object which performs
                named entity recognition given a model.
        """
        print("*** Creating and loading a NER pipeline.")
        # Path to NER Model:
        model_name = self.model
        # If the model is local (has not been obtained from the hub),
        # pre-append the model path and the extension of the model
        # to obtain the model name.
        if self.load_from_hub == False:
            model_name = self.model_path + self.model + ".model"
        # Load a NER pipeline:
        self.pipe = pipeline("ner", model=model_name, ignore_labels=[])
        return self.pipe

    # -------------------------------------------------------------
    def ner_predict(self, sentence):
        """
        Given a sentence, recognise its mentioned entities.

        Arguments:
            sentence (str): a sentence.

        Returns:
            predictions (list): a list of dictionaries, one per recognised
            token, e.g.: {'entity': 'O', 'score': 0.99975187, 'word': 'From',
            'start': 0, 'end': 4}
        """

        # The n-dash is a very frequent character in historical newspapers,
        # but the NER pipeline does not process it well: Plymouth—Kingston
        # is parsed as "Plymouth (B-LOC), — (B-LOC), Kingston (B-LOC)", instead
        # of the n-dash being interpreted as a word separator. Therefore, we
        # replace it by a comma, except when the n-dash occurs in the opening
        # position of a sentence.
        if len(sentence) <= 1:  # Error if the sentence is too short.
            return []
        sentence = sentence[0] + sentence[1:].replace("—", ",")
        # Run the NER pipeline to predict mentions:
        ner_preds = self.pipe(sentence)
        # Post-process the predictions, fixing potential grouping errors:
        lEntities = []
        predictions = []
        for pred_ent in ner_preds:
            prev_tok = pred_ent["word"]
            pred_ent["score"] = float(pred_ent["score"])
            pred_ent["entity"] = pred_ent["entity"]
            pred_ent = ner.fix_capitalization(pred_ent, sentence)
            if prev_tok.lower() != pred_ent["word"].lower():
                print("Token processing error.")
            predictions = ner.aggregate_entities(pred_ent, lEntities)
        if len(predictions) > 0:
            predictions = ner.fix_hyphens(predictions)
            predictions = ner.fix_nested(predictions)
            predictions = ner.fix_startEntity(predictions)
        return predictions

import json
import os
import sqlite3
import sys
from array import array
from ast import literal_eval
from typing import Any, List, Literal, Optional

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.pardir))
from geoparser import ranking

RANDOM_SEED = 42
"""Constant representing the random seed used for generating pseudo-random
numbers.

The `RANDOM_SEED` is a value that initializes the random number generator
algorithm, ensuring that the sequence of random numbers generated remains the
same across different runs of the program. This is useful for achieving
reproducibility in experiments or when consistent random behavior is
desired.

..
    If this docstring is changed, also make sure to edit prepare_data.py,
    linking.py, entity_disambiguation.py.
"""
np.random.seed(RANDOM_SEED)


def get_db_emb(
    cursor: sqlite3.Cursor,
    mentions: List[str],
    embtype: Literal["word", "entity", "snd"],
) -> List[Optional[np.ndarray]]:
    """
    Retrieve Wikipedia2Vec embeddings for a given list of words or entities.

    Arguments:
        cursor: The cursor with the open connection to the Wikipedia2Vec
            database.
        mentions (List[str]): The list of words or entities whose embeddings to
            extract.
        embtype (Literal["word", "entity", "snd"]): The type of embedding to
            retrieve. Possible values are ``"word"``, ``"entity"``, or
            ``"snd"``. If it is set to ``"word"`` or ``"snd"``, we use
            Wikipedia2Vec word embeddings, if it is set to ``"entity"``, we
            use Wikipedia2Vec entity embeddings.

    Returns:
        List[Optional[np.ndarray]]:
            A list of arrays (or ``None``) representing the embeddings for the
            given mentions.

    Note:
        - The embeddings are extracted from the Wikipedia2Vec database using
          the provided cursor.
        - If the mention is an entity, the prefix ``ENTITY/`` is preappended to
          the mention before querying the database.
        - If the mention is a word, the string is converted to lowercase
          before querying the database.
        - If an embedding is not found for a mention, the corresponding
          element in the returned list is set to None.
        - Differently from the original REL implementation, we use Wikipedia2vec
          embeddings both for ``"word"`` and ``"snd"``.
    """

    results = []
    for mention in mentions:
        result = None
        # Preprocess the mention depending on which embedding to obtain:
        if embtype == "entity":
            if not mention == "#ENTITY/UNK#":
                mention = "ENTITY/" + mention
            result = cursor.execute(
                "SELECT emb FROM entity_embeddings WHERE word=?", (mention,)
            ).fetchone()
        if embtype == "word" or embtype == "snd":
            if mention in ["#WORD/UNK#", "#SND/UNK#"]:
                mention = "#WORD/UNK#"
            else:
                mention = mention.lower()
            result = cursor.execute(
                "SELECT emb FROM entity_embeddings WHERE word=?", (mention,)
            ).fetchone()
        results.append(result if result is None else array("f", result[0]).tolist())

    return results


def eval_with_exception(str2parse: str, in_case: Optional[Any] = "") -> Any:
    """
    Parse a string in the form of a list or dictionary.

    Arguments:
        str2parse (str): The string to parse.
        in_case (str, optional): The value to return in case of an error.
            Default is ``""``.

    Returns:
        Any
            The parsed value if successful, or the specified value in case of
            an error.
    """
    try:
        return literal_eval(str2parse)
    except ValueError:
        return in_case


def prepare_initial_data(df: pd.DataFrame) -> dict:
    """
    Generate the initial JSON data needed to train a REL model from a
    DataFrame.

    Arguments:
        df: The dataframe containing the linking training data.

    Returns:
        dict:
            A dictionary with article IDs as keys and a list of mention
            dictionaries as values. Each mention dictionary contains
            information about a mention, excluding the "gold" field and
            candidates (at this point).

    Note:
        The DataFrame passed to this function can be generated by the
        ``experiments/prepare_data.py`` script.
    """
    dict_mentions = dict()
    for i, row in df.iterrows():
        article_id = str(row["article_id"])
        dict_sentences = dict()
        for s in eval_with_exception(row["sentences"]):
            dict_sentences[int(s["sentence_pos"])] = s["sentence_text"]

        # Build a mention dictionary per mention:
        for df_mention in eval_with_exception(row["annotations"]):
            dict_mention = dict()
            mention = df_mention["mention"]
            sent_idx = int(df_mention["sent_pos"])
            sentence_id = article_id + "_" + str(sent_idx)

            # Generate left-hand context:
            left_context = ""
            if sent_idx - 1 in dict_sentences:
                left_context = dict_sentences[sent_idx - 1]

            # Generate right-hand context:
            right_context = ""
            if sent_idx + 1 in dict_sentences:
                right_context = dict_sentences[sent_idx + 1]

            dict_mention["mention"] = df_mention["mention"]
            dict_mention["sent_idx"] = sent_idx
            dict_mention["sentence"] = dict_sentences[sent_idx]
            dict_mention["ngram"] = mention
            dict_mention["context"] = [left_context, right_context]
            dict_mention["pos"] = df_mention["mention_start"]
            dict_mention["end_pos"] = df_mention["mention_end"]
            dict_mention["place"] = row["place"]
            dict_mention["place_wqid"] = row["place_wqid"]
            dict_mention["candidates"] = []
            dict_mention["ner_label"] = df_mention["entity_type"]

            # Check this:
            dict_mention["gold"] = [df_mention["wkdt_qid"]]
            if not df_mention["wkdt_qid"].startswith("Q"):
                dict_mention["gold"] = "NIL"

            if sentence_id in dict_mentions:
                dict_mentions[sentence_id].append(dict_mention)
            else:
                dict_mentions[sentence_id] = [dict_mention]

    return dict_mentions


def rank_candidates(rel_json: dict, wk_cands: dict, mentions_to_wikidata: dict) -> dict:
    """
    Rank the candidates for each mention in the provided JSON data.

    Arguments:
        rel_json (dict): The JSON data containing articles and mention
            information.
        wk_cands (dict): Dictionary of Wikidata candidates for each mention.
        mentions_to_wikidata (dict): Dictionary mapping mentions to Wikidata
            entities.

    Returns:
        dict: A new JSON dictionary with ranked candidates for each mention.
    """
    new_json = dict()
    for article in rel_json:
        new_json[article] = []
        for mention_dict in rel_json[article]:
            cands = []
            tmp_cands = []
            max_cand_freq = 0
            ranker_cands = wk_cands.get(mention_dict["mention"], dict())
            for c in ranker_cands:
                # DeezyMatch confidence score (cosine similarity):
                cand_selection_score = ranker_cands[c]["Score"]
                # For each Wikidata candidate:
                for qc in ranker_cands[c]["Candidates"]:
                    # Mention-to-wikidata absolute relevance:
                    qcrlv_score = mentions_to_wikidata[c][qc]
                    if qcrlv_score > max_cand_freq:
                        max_cand_freq = qcrlv_score
                    qcm2w_score = ranker_cands[c]["Candidates"][qc]
                    # Average of CS conf score and mention2wiki norm relv:
                    if cand_selection_score:
                        qcm2w_score = (qcm2w_score + cand_selection_score) / 2
                    tmp_cands.append((qc, qcrlv_score, qcm2w_score))
            # Append candidate and normalized score weighted by candidate selection conf:
            for cand in tmp_cands:
                qc_id = cand[0]
                # Normalize absolute mention-to-wikidata relevance by entity:
                qc_score_1 = cand[1] / max_cand_freq
                # Candidate selection confidence:
                qc_score_2 = cand[2]
                # Averaged relevances and normalize between 0 and 0.9:
                qc_score = ((qc_score_1 + qc_score_2) / 2) * 0.9
                cands.append([qc_id, round(qc_score, 3)])
            # Sort candidates and normalize between 0 and 1, and so they add up to 1.
            cands = sorted(cands, key=lambda x: (x[1], x[0]), reverse=True)

            mention_dict["candidates"] = cands
            new_json[article].append(mention_dict)
    return new_json


def add_publication(
    rel_json: dict, publname: Optional[str] = "", publwqid: Optional[str] = ""
) -> dict:
    """
    Add publication information to the provided JSON data.

    Arguments:
        rel_json (dict): The JSON data containing articles and mention
            information.
        publname (str, optional): The name of the publication. Defaults to an
            empty string.
        publwqid (str, optional): The Wikidata ID of the publication. Defaults
            to an empty string.

    Returns:
        dict: A new JSON dictionary with the added publication information.
    """
    new_json = rel_json.copy()
    for article in rel_json:
        place = publname
        place_wqid = publwqid
        if article != "linking":
            place = rel_json[article][0].get("place", publname)
            place_wqid = rel_json[article][0].get("place_wqid", publwqid)
        preffix_sentence = "This article is published in "
        sentence = preffix_sentence + place + "."
        dict_publ = {
            "mention": place,
            "sent_idx": 0,
            "sentence": sentence,
            "gold": [place_wqid],
            "ngram": place,
            "context": ["", ""],
            "pos": len(preffix_sentence),
            "end_pos": len(preffix_sentence + sentence),
            "candidates": [[place_wqid, 1.0]],
            "place": place,
            "place_wqid": place_wqid,
            "ner_label": "LOC",
        }
        new_json[article].append(dict_publ)
    return new_json


def prepare_rel_trainset(
    df: pd.DataFrame,
    rel_params,
    mentions_to_wikidata,
    myranker: ranking.Ranker,
    dsplit: str,
) -> dict:
    """
    Prepare the data for training and testing a REL disambiguation model.

    This function takes as input a pandas DataFrame (`df`) containing the
    dataset generated in the ``experiments/prepare_data.py`` script, along
    with a Linking object (``mylinker``) and a Ranking object (``myranker``).
    It prepares the data in the format required to train and test a REL
    disambiguation model, using the candidates from the ranker.

    Arguments:
        df (pandas.DataFrame): The pandas DataFrame containing the prepared
            dataset.
        rel_params (dict): Dictionary containing the parameters for performing
            entity disambiguation using the ``reldisamb`` approach.
        mentions_to_wikidata (dict): Dictionary mapping mentions to Wikidata
            entities, with counts.
        myranker (geoparser.ranking.Ranker): The Ranking object.
        dsplit (str): The split identifier for the data (e.g., ``"train"``,
            ``"test"``).

    Returns:
        dict: The prepared data in the format of a JSON dictionary.

    Note:
        This function stores the formatted dataset as a JSON file.
    """
    rel_json = prepare_initial_data(df)

    # Get unique mentions, to run them through the ranker:
    all_mentions = []
    for article in rel_json:
        if rel_params["without_microtoponyms"]:
            all_mentions += [
                y["mention"] for y in rel_json[article] if y["ner_label"] == "LOC"
            ]
        else:
            all_mentions += [y["mention"] for y in rel_json[article]]
    all_mentions = list(set(all_mentions))
    # Format the mentions are required by the ranker:
    all_mentions = [{"mention": mention} for mention in all_mentions]
    # Use the ranker to find candidates:
    wk_cands, myranker.already_collected_cands = myranker.find_candidates(all_mentions)
    # Rank the candidates:
    rel_json = rank_candidates(
        rel_json,
        wk_cands,
        mentions_to_wikidata,
    )
    # If "publ" is taken into account for the disambiguation, add the place
    # of publication as an additional already disambiguated entity per row:
    if rel_params["with_publication"] == True:
        rel_json = add_publication(
            rel_json,
            rel_params["default_publname"],
            rel_params["default_publwqid"],
        )

    ## TO DO
    with open(
        os.path.join(rel_params["data_path"], "rel_{}.json").format(dsplit),
        "w",
    ) as f:
        json.dump(rel_json, f)

    return rel_json

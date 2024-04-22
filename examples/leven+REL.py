import scipy
print(scipy.__version__)
import spacy
print(spacy.__version__)
import json
import os
import sys
import sqlite3
from pathlib import Path
import json
import time

import pandas as pd
import WikidataObject as wdo

from t_res.geoparser import geode_pipe,ranking,linking
path = '/home/antoine/Documents/GitHub/T-Res/'
path = '/home/jovyan/T-Res/'
NER_path = path + 'resources/fr_spacy_custom_spancat_edda'


# Import VILLESFR
filepath = 'VILLESFR.json'
VILLESFR = pd.read_json(filepath, orient='records', lines=True)


def load_resources(method="mostpopular",
                   resources_path="../resources/"
                   ) :

    print("*** Loading the ranker resources.")

    # Load files
    files = {
        "mentions_to_wikidata": os.path.join(
            resources_path, "wikidata/mentions_to_wikidata_normalized.json"
        ),
        "wikidata_to_mentions": os.path.join(
            resources_path, "wikidata/wikidata_to_mentions_normalized.json"
        ),
    }

    with open(files["mentions_to_wikidata"], "r") as f:
        mentions_to_wikidata = json.load(f)

    with open(files["wikidata_to_mentions"], "r") as f:
        wikidata_to_mentions = json.load(f)

    # Filter mentions to remove noise:
    wikidata_to_mentions_filtered = dict()
    mentions_to_wikidata_filtered = dict()
    for wk in wikidata_to_mentions:
        wikipedia_mentions = wikidata_to_mentions.get(wk)
        wikipedia_mentions_stripped = dict(
            [
                (x, wikipedia_mentions[x])
                for x in wikipedia_mentions
                if not ", " in x and not " (" in x
            ]
        )

        if wikipedia_mentions_stripped:
            wikipedia_mentions = wikipedia_mentions_stripped

        wikidata_to_mentions_filtered[wk] = dict(
            [(x, wikipedia_mentions[x]) for x in wikipedia_mentions]
        )

        for m in wikidata_to_mentions_filtered[wk]:
            if m in mentions_to_wikidata_filtered:
                mentions_to_wikidata_filtered[m][
                    wk
                ] = wikidata_to_mentions_filtered[wk][m]
            else:
                mentions_to_wikidata_filtered[m] = {
                    wk: wikidata_to_mentions_filtered[wk][m]
                }

    mentions_to_wikidata = mentions_to_wikidata_filtered
    wikidata_to_mentions = wikidata_to_mentions_filtered

    del mentions_to_wikidata_filtered
    del wikidata_to_mentions_filtered

    # Parallelize if ranking method is one of the following:
    if method in ["partialmatch", "levenshtein"]:
        pandarallel.initialize(nb_workers=10)
        os.environ["TOKENIZERS_PARALLELISM"] = "true"

    return mentions_to_wikidata, wikidata_to_mentions

mentions_to_wikidata, wikidata_to_mentions = load_resources()


# In[6]:


df = VILLESFR.copy(deep=True)
df['related_mentions'] = None

for idx,row in df.iterrows():
    related_mentions = []
    # check if wikidata_to_mentions.get(row['gold']) doesn't yeld a keyerror :
    if wikidata_to_mentions.get(row['gold']) is None:
        df.at[idx,'related_mentions'] = None
        #print(row['head'])
        #print(WDO.WikidataObject(row['gold']))
        continue
    else:
        related_mentions = wikidata_to_mentions.get(row['gold'])
        #print(row['head'])
        #print(related_mentions)
        df.at[idx,'related_mentions'] = related_mentions

print('count of None in related_mentions : ', df['related_mentions'].isnull().sum())
        

subVILLESFR = df[df['related_mentions'].notnull()]
subVILLESFR.shape


# # dam-lev + REL

print('#######################################################################')
print('####################  DAM-LEV +  REL        ####################')

# --------------------------------------
# Instantiate the ranker:
myranker = ranking.Ranker(
    method="levenshtein",
    resources_path="../resources/",
)

# --------------------------------------
# Instantiate the Linker:
with sqlite3.connect( path + "resources/rel_db/embeddings_database.db") as conn:
    cursor = conn.cursor()
    mylinker = linking.Linker(
        method="reldisamb",
        resources_path= path + "resources/",
        rel_params={
            "model_path": path + "resources/models/disambiguation/",
            "data_path":  path + "experiments/outputs/data/lwm/",
            "training_split": "originalsplit",
            "db_embeddings": cursor,
            "with_publication": False,
            "without_microtoponyms": True,
            "do_test": False,
            "default_publname": "",
            "default_publwqid": "",
        },
        overwrite_training=False,
    )


# In[ ]:


geoparser = geode_pipe.Pipeline(geodeNERpath=NER_path,
                              myranker=myranker,
                              mylinker=mylinker)


# In[6]:

print('#######################################################################')
print('####################  test       ####################')

sentence = "* ALBI, (Géog.) ville dans le haut Languedoc "
print(sentence)
resolved = geoparser.run_sentence(sentence, HEAD='ALBI', verbose=False)
for r in resolved:
    print(json.dumps(r, indent=2))


# In[ ]:
print('#######################################################################')
print('####################  villesFR      ####################')

sample2 = subVILLESFR.copy(deep=True)
verbose = False
start = time.time()

if 'resolved' not in sample2.columns:
    sample2['resolved'] = None

for i, row in sample2.iterrows():
    resolved = geoparser.run_sentence(row['fullcontent'], HEAD=row['head'])

    skyline = row['gold'] in resolved[0]['cross_cand_score'].keys()
    best_pred = resolved[0]['prediction']

    if best_pred[0] != 'Q':
        acc10 = False
    else:
        wd_pred = wdo.WikidataObject(best_pred, coordinates=resolved[0]['latlon'])
        acc10 = wd_pred._distance_to(row['gold']) <= 10

    sample2.at[i, 'resolved'] = resolved[0]
    sample2.at[i, 'skyline'] = skyline
    sample2.at[i, 'bestPred'] = best_pred
    sample2.at[i, 'acc10'] = acc10

    if verbose:
        print(f"Head: {row['head']}")
        print(f"Gold: {row['gold']}")
        print(f"Prediction: {best_pred}")
        print(f"Skyline: {skyline}")
        print(resolved[0]['cross_cand_score'])
        print(f"Accuracy 10: {acc10}\n")

print(time.time() - start)
print()


# In[ ]:


sample2.to_json('DAMLEV+rel_220424.json', orient='records', lines=True)


# In[10]:


print(sample2.resolved.apply(lambda x: len(x['cross_cand_score'])).describe())
print()
print(sample2.skyline.value_counts(normalize=True), '\n')
print(sample2.acc10.value_counts(normalize=True), '\n')


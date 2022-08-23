import json
import os
import sys

import pytest

# Add "../" to path to import utils
sys.path.insert(0, os.path.abspath(os.path.pardir))
from geoparser import ranking


def test_initialise_method():
    """
    Test initialisation works fine
    """
    myranker = ranking.Ranker(
        method="perfectmatch",
        resources_path="/resources/wikidata/",
        mentions_to_wikidata=dict(),
        wikidata_to_mentions=dict(),
    )
    assert type(myranker.__str__()) == str


def test_load_resources():
    """
    Tests resources are loaded and processed correctly
    """
    with open("/resources/wikidata/mentions_to_wikidata_normalized.json", "r") as f:
        mentions_to_wikidata = json.load(f)

    myranker = ranking.Ranker(
        method="perfectmatch",
        resources_path="/resources/wikidata/",
        mentions_to_wikidata=dict(),
        wikidata_to_mentions=dict(),
        wiki_filtering={
            "top_mentions": 3,  # Filter mentions to top N mentions
            "minimum_relv": 0.03,  # Filter mentions with more than X relv
        },
    )
    filtered_mentions = myranker.load_resources()
    assert len(filtered_mentions) < len(mentions_to_wikidata)
    assert len(filtered_mentions["London"]) < len(mentions_to_wikidata["London"])
    assert len(filtered_mentions["London"]) > 0
    assert filtered_mentions["London"]["Q84"] == mentions_to_wikidata["London"]["Q84"]


def test_perfect_match():
    """
    Test that perfect_match returns only perfect matching cases
    """
    myranker = ranking.Ranker(
        method="perfectmatch",
        resources_path="/resources/wikidata/",
        mentions_to_wikidata=dict(),
        wikidata_to_mentions=dict(),
        wiki_filtering={
            "top_mentions": 3,  # Filter mentions to top N mentions
            "minimum_relv": 0.03,  # Filter mentions with more than X relv
        },
    )
    myranker.load_resources()
    candidates, already_collected_cands = myranker.perfect_match(["London"])
    assert candidates["London"]["London"] == 1.0

    candidates, already_collected_cands = myranker.perfect_match(["Lvndon"])
    assert candidates["Lvndon"] == {}

    candidates, already_collected_cands = myranker.perfect_match(["Paperopoli"])
    assert candidates["Paperopoli"] == {}


def test_damlev():
    """
    Test that damlev returns correctly
    """
    myranker = ranking.Ranker(
        method="partialmatch",
        resources_path="/resources/wikidata/",
        mentions_to_wikidata=dict(),
        wikidata_to_mentions=dict(),
        wiki_filtering={
            "top_mentions": 3,  # Filter mentions to top N mentions
            "minimum_relv": 0.03,  # Filter mentions with more than X relv
        },
    )
    score = myranker.damlev_dist("Lvndon", {"mentions": "London"})
    assert score == 0.8333333283662796

    with pytest.raises(TypeError):
        found = True
        myranker.damlev_dist("Lvndon", "London")
    assert found == True

    assert 0.0 == myranker.damlev_dist("uityity", {"mentions": "asdasd"})


def test_check_if_contained():
    """
    Test that check_if_contained returns score only when there is an overlap
    """

    myranker = ranking.Ranker(
        method="partialmatch",
        resources_path="/resources/wikidata/",
        mentions_to_wikidata=dict(),
        wikidata_to_mentions=dict(),
        wiki_filtering={
            "top_mentions": 3,  # Filter mentions to top N mentions
            "minimum_relv": 0.03,  # Filter mentions with more than X relv
        },
    )
    score_a = myranker.check_if_contained("New York", {"mentions": "New York City"})
    score_b = myranker.check_if_contained("New York City", {"mentions": "New York"})

    assert score_a == score_b == 0.6153846153846154

    with pytest.raises(TypeError):
        found = True
        myranker.check_if_contained("Lvndon", "London")
    assert found == True

    assert None == myranker.check_if_contained("London", {"mentions": "New York"})

import pytest

from parrotpy.analyzer import NLPSingleton

# test Named Entity Recognition (NER)
  
def test_spacy():
    # python -m spacy download en_core_web_sm
    nlp1 = NLPSingleton()

    texts = [
        "Apple is looking at buying U.K. startup for $1 billion.",
        "Alice Anderson",
        "Allison Beer",
        "Betty Ford",
        "Charlie Chapman",
        "David Letterman",
        "Eddie Young",
        "123 Spring Street, Springfield OH 10001",
        "Band of America",
        "General Motor",
        "Fast Fashion Inc."
    ]

    cat = nlp1.categorize(texts)
    assert(cat == "PERSON")
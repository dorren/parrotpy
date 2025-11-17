
build:
	poetry install

test:
	pytest -s tests

dl_spacy_model:
	python -m spacy download en_core_web_sm
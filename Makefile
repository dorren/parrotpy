
build:
	poetry install

test:
	pytest -s tests

dl_spacy_model:
	python -m spacy download en_core_web_sm

clean:
	-DEL /Q dist\*.whl
	-DEL /Q dist\*.tar.gz

pkg:
	poetry -m build .

publish_test:
	poetry publish --build --repository testpypi
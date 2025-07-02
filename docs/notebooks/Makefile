clear:
	rm -rf build

build: clear
	uv run marimo -y export html-wasm transformations/transformations.py -o build/transformations --mode edit

serve: build
	python -m http.server --directory build
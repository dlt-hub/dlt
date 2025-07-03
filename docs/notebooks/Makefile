clear:
	rm -rf build

build: clear
	uv run marimo -y export html-wasm playground/playground.py -o build/playground --mode edit

serve: build
	python -m http.server 9000 --directory build

test:
	uv run python playground/playground.py

uninstall-example-plugin:
	pip uninstall example_plugin -y

install-example-plugin: uninstall-example-plugin
	# this builds and installs the example plugin
	poetry build
	pip install dist/example_plugin-0.1.0-py3-none-any.whl
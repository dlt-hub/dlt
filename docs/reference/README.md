# dlt API Reference Documentation

This directory contains the Sphinx-based API reference documentation for the dlt library.

## Structure

- `conf.py` - Sphinx configuration file
- `index.rst` - Main entry point for the documentation
- `Makefile` - Build scripts for documentation

## Building the Documentation

To build the documentation:

1. Ensure you have the required dependencies:
   ```
   pip install sphinx sphinx-rtd-theme
   ```

2. Run the build command:
   ```
   cd docs/reference
   make html
   ```

3. The generated documentation will be available in `_build/html/`

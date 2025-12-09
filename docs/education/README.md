# Adding New Notebooks

## Overview

The `.py` files in this directory are **auto-generated** from `.ipynb` files. Only edit the `.ipynb` files.

To regenerate `.py` files:
```bash
make build-molabs
```

Preprocessing logic: [`docs/docs_tools/education/`](../docs_tools/education/)

## Things to consider

To ensure compatibility with both **Google Colab** and **Marimo/Molab**:

### 1. **No inline comments**
Bad: `x = 5  # comment`  
Good: Separate line comments

**Why:** `marimo convert` scatters inline comments

## Workflow

1. Create/edit `.ipynb` in the course folder
2. Follow guidelines above
3. Run `make build-molabs` to generate `.py` files
4. Test both versions (Colab and Molab)
5. Commit both `.ipynb` and `.py` files
6. Make changes to the processing logic in `docs/docs_tools/education/` if necessary.
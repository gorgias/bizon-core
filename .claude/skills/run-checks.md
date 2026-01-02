---
description: Run formatting, linting, and tests before committing
---

# Run Pre-Commit Checks

Run the standard checks before committing code.

## Quick Command

```bash
make format && uv run pytest tests/ -v
```

## Individual Commands

### Format Code
```bash
uv run ruff format .
```

### Lint and Auto-Fix
```bash
uv run ruff check --fix .
```

### Run All Tests
```bash
uv run pytest tests/ -v
```

### Run Specific Tests
```bash
# Single file
uv run pytest tests/path/to/test_file.py -v

# Single test
uv run pytest tests/path/to/test_file.py -k "test_name" -v

# With coverage
uv run pytest --cov=bizon tests/
```

## Makefile Shortcuts

```bash
make format    # Ruff format + lint
make lint      # Lint only
make test      # Run pytest
make install   # Full dev install
```

## Common Fixes

### Import Order Issues
Ruff handles this automatically with `--fix`

### Line Too Long (>120 chars)
- Use parentheses for continuation
- Break long strings
- Add `# noqa: E501` if unavoidable

### Type Hints
Use `List`, `Dict`, `Optional` from `typing` for Python 3.9 compatibility

## Before Committing

1. Run `make format`
2. Run `uv run pytest`
3. Check for any remaining linting issues
4. Commit with conventional message format:
   - `feat(source): add Stripe connector`
   - `fix(destination): handle timeout`
   - `docs: update contributing guide`

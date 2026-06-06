# Contributing to shifting_work_hours

Thank you for your interest in contributing to this project!

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/<your-username>/shifting_work_hours.git`
3. Create a virtual environment: `python -m venv .venv`
4. Install in development mode: `pip install -e ".[dev]"`
5. Create a branch: `git checkout -b feature/your-feature`

## Development Setup

```bash
# Install package in editable mode with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Run code formatting
black src/ tests/ scripts/

# Run linting
ruff check src/ tests/ scripts/
```

## Project Structure

- `src/shifting_work_hours/` - Main package
  - `config/` - Configuration and constants
  - `core/` - Core utilities (status tracking, task runner, I/O)
  - `pipeline/` - Processing pipeline stages
  - `utils/` - Helper utilities
- `scripts/` - CLI entry points
- `tests/` - Test suite
- `nasa_climate_data/` - Scrapy spider for NASA data catalog

## Adding a New Pipeline Stage

1. Create a new module in `src/shifting_work_hours/pipeline/`
2. Implement a `run()` function following the existing pattern
3. Add a CLI subcommand in `scripts/run_pipeline.py`
4. Add tests in `tests/`
5. Update the README

## Code Style

- Use [Black](https://github.com/psf/black) for formatting (line length: 100)
- Use [Ruff](https://github.com/astral-sh/ruff) for linting
- Follow existing code patterns and naming conventions
- Add docstrings to all public functions

## Submitting Changes

1. Ensure all tests pass: `pytest tests/`
2. Commit your changes with a clear message
3. Push to your fork and open a Pull Request

## Reporting Issues

Please use GitHub Issues to report bugs or suggest features. Include:
- A clear description of the issue
- Steps to reproduce (for bugs)
- Expected vs actual behavior
- Your environment (OS, Python version, GPU if relevant)

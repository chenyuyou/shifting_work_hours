"""Thin wrapper for backward compatibility.

The CLI has moved to shifting_work_hours.cli. After installing the package
(pip install -e .), you can use the console command directly:

    shifting-wh <command> [options]

Or run via Python:

    python -m shifting_work_hours <command> [options]

This script is kept for convenience during development.
"""

from shifting_work_hours.cli import main

if __name__ == '__main__':
    main()

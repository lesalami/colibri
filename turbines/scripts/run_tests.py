import sys
import pytest

if __name__ == "__main__":
    exit_code = pytest.main(["-v", "tests"])
    sys.exit(exit_code)
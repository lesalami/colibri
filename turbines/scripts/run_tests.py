import pytest
import sys

exit_code = pytest.main(["-v", "tests"])

sys.exit(exit_code)
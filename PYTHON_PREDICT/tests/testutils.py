import config
import os
import shutil

# for asserting it exits the program
def assertExit(test_func, expected_code=1):
    try:
        test_func()
    except SystemExit as e:
        assert e.code == expected_code, f"Expected exit code {expected_code}, got {e.code}"
    else:
        raise AssertionError("SystemExit was not raised")

'''
# Creates folders for test
def setup_tmp_dirs():
    
    for folder in [config.TMPF, config.TMPD]:
        if os.path.exists(folder):
            shutil.rmtree(folder)

def assertRaises(exception_type, func, *args, **kwargs):
    try:
        func(*args, **kwargs)
        assert False, f"{exception_type.__name__} not raised"
    except exception_type:
        pass
'''
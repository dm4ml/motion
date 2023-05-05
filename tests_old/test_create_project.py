# Tests the ability to create a project and run it.

import motion
import os
import shutil

from subprocess import run


def test_create_project(entry):
    os.makedirs("/tmp/motionapps", exist_ok=True)
    os.makedirs("/tmp/motion", exist_ok=True)
    os.chdir("/tmp/motionapps")

    # Create a project
    motion.create_app(name="testproj", author="testauthor")
    os.chdir("/tmp/motionapps/testproj")

    # Execute the test.py file in the testproject directory
    run(["python", "mconfig.py"])

    shutil.rmtree("/tmp/motionapps/testproj")

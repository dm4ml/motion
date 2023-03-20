from setuptools import setup, find_packages

import pathlib

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()

setup(
    name="motion",
    version="0.1",
    description="A framework for building ML applications.",
    long_description=README,
    long_description_content_type="text/markdown",
    author="shreyashankar",
    author_email="shreyashankar@berkeley.edu",
    license="Apache License 2.0",
    classifiers=[
        "Intended Audience :: Developers",
        "Programming Language :: Python",
    ],
    packages=find_packages(exclude=["tests"]),
    install_requires=[
        "dill",
        "duckdb",
        "click",
        "croniter",
        "fastapi",
        "pandas",
        "pytest",
    ],
    entry_points="""
        [console_scripts]
        motion=motion.cli:motioncli
    """,
    include_package_data=True,
)

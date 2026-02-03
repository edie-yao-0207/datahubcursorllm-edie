from setuptools import find_packages, setup

setup(
    name="datamodel",
    packages=find_packages(exclude=["datamodel_tests"]),
    install_requires=["dagster", "dagster-cloud"],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)

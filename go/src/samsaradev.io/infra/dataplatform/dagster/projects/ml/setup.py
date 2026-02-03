from setuptools import find_packages, setup

setup(
    name="ml",
    packages=find_packages(exclude=["ml_tests"]),
    install_requires=["dagster", "dagster-cloud"],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
    python_requires=">=3.10",
)

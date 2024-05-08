from setuptools import find_packages, setup

setup(
    name="dagster_kafka_demo",
    packages=find_packages(),
    install_requires=["dagster", "dagster-cloud", "kafka-python"],
    extras_require={"dev": ["dagit", "pytest", "ruff"]},
)

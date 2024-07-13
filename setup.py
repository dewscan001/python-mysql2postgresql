from setuptools import setup, find_packages


def readme():
    with open(
        "README.md",
        "r",
        encoding="utf8",
    ) as f:
        return f.read()


setup(
    name="python-mysql2postgresql",
    packages=find_packages(),
    version="0.1.2",
    description="python-mysql2postgresql",
    author="DewBloodmetal, FMalina",
    author_email="dewscan001@gmail.com",
    install_requires=["mysql-connector-python", "psycopg2", "tqdm", "fire"],
    keywords="python mysql2postgresql python-mysql2postgresql",
    long_description=readme(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Topic :: Database",
    ],
)

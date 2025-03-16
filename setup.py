from setuptools import setup, find_packages

setup(
    name="big-data-projet-ensta",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "pyspark",
    ],
    author="Quentin Loriaux, Jérome Farges",
    description="Outils pour l'analyse de reviews et système de recommandation de jeux",
    long_description=open("readme.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/QuentinLoriaux/big-data-projet-ensta",
)

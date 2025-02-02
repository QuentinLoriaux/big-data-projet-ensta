# Projet Big-Data ENSTA Paris 2025

Dataset : [Steam reviews](https://www.kaggle.com/datasets/andrewmvd/steam-reviews)

Auteurs :
- Quentin Loriaux
- Jérome Farges

## Installation

- Installer [Spark](https://dlcdn.apache.org/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz) :
```
mv ~/Downloads/spark-3.5.4-bin-hadoop3.tgz ~
unzip ~/spark-3.5.4-bin-hadoop3.tgz
rm ~/spark-3.5.4-bin-hadoop3.tgz
```

Dans le .bashrc :
```
export SPARK_HOME=/home/$USER/spark-3.5.4-bin-hadoop3
export PATH=$SPARK_HOME:$PATH

```

- télécharger `archive.zip` [ici](https://www.kaggle.com/datasets/andrewmvd/steam-reviews)

```
cd dataset
pyenv virtualenv big-data
pyenv activate big-data
pip install pyspark
cp ~/Downloads/archive.zip .
unzip archive.zip
mv dataset.csv steam_reviews.csv
rm archive.zip
cd ..

```

## Profit

Stonks
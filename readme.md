# Projet Big-Data ENSTA Paris 2025

Datasets :
- [Steam reviews](https://www.kaggle.com/datasets/andrewmvd/steam-reviews)
- [users](https://www.kaggle.com/datasets/bossadapt/public-steam-users-reviews-games-and-friends)
- [swearWords.txt](http://www.bannedwordlist.com/lists/swearWords.txt)

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
pyenv virtualenv big-data-project
pyenv activate big-data-project
pip install -e .
cd dataset
cp ~/Downloads/archive.zip .
unzip archive.zip
mv dataset.csv steam_reviews.csv
rm archive.zip
cd ..

```

- De même, télécharger et désarchiver dans `dataset` les autres datasets.

- Pour les tests unitaires, aller dans `./src/projet` et faire `python <fichier>.py <typeFichier>`, par exemple :

```
python review_ranking.py csv
python insult_ranking.py parquet 
```

- Pour exécuter des tests généraux avec prompts : lancer `python ./src/main.py`

## Expériences

- Classement des jeux selon les reviews
    - Calcul de la différence entre le nombre d'avis positifs et négatifs
    - Calcul de la différence entre le nombre de mots total des avis positifs et négatifs

- Classement des insultes les plus fréquentes dans les reviews
    - Approche avec PySpark
    - Approche avec MapReduce

- Création d'un système de recommandation à partir des utilisateurs
    - Etablissement d'un score de recommandation global des jeux : $ \text{score} = \text{total\_recommandations} \times \log(1 + \text{total\_heures\_jouees}) $
    - Etablissement d'un classement des 3 jeux les plus fréquents chez les amis d'un utilisateur.


## Résultats

Creation de la session spark : 2.2s.
La recreation est instantanee, peut-etre lie a la compilation java : JVM warm up
spark.catalog.clearCache() et spark.stop() entre chaque exp => pas d'impact notable sur les perfs


lecture du fichier : partie la plus longue (2s pour parquet, 6s pour CSV)
df.unpersist() => les operations suivantes devraient moins exploiter le cache mais visiblement, le fichier est toujours en memoire a cause du cache systeme (4s => 0.1s)

Les specs de la machine utilisée:
Kernel : Linux 6.12.13-amd64
Architecture: x86-64
Firmware Version: F.32

Durée exacte de créer sparksession
Faire un tableau
                                    Parquet         CSV
naive notation (sans cache)  ~2s  ~7s
naive notation (Cache)
token aware notation (sans cache) ~ 2s ~ 7s
token aware notation (cache)
(décroit énormément quand fait deux fois d'affilé, ~ 0.1 - 0.2s / ~3.5s  pour la deuxième)

spark insult (sans cache) ~2s ~ 7s
spark insult (cache)
map reduce insult (sans cache) ~46s ~52s
map reduce insult (cache)


On prend soin de vider autant que possible la mémoire pour ne pas crash pour les recommandations

Autre tableau
                            batch size = 10 000, partition_nb = 10          batch size = ?, partition_nb = ?
global recsys 
user_specific recsys





### Interprétations



## Pistes pour de futures avancées

toto
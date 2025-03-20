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

## Description des fonctions
Les fonctions utilisées sont celles de src/projet.
Ces fonctions sont issues de celles présentes dans src/test, qui sont des versions non optimisées.

\__init__.py crée la fonction benchmark, qui crée la fonction benchmark permettant d'évaluer la durée d'exécution des différentes fonctions.

benchmark.sh génère 20 tests pour chaque fonctions de insult_ranking et review_ranking en csv et en parquet.

ingest.py convertie un fichier csv en fichier parquet.

insult_ranking.py génère un classement des insultes les plus fréquemment untilisées dans les revues steam, par un map_reduce ou par spark.

recommandation.py génère une liste de jeux recommandés pour les utilisateurs, par un classement de popularité des jeux (méthode de recommandation non ciblée) ou en utilisant les jeux les plus fréquents des listes d'amis des utilisateurs (méthode de recommandation ciblée).

review_ranking.py génère un classement des jeux par popularité, par une méthode naive et une méthode enregistrant les tokens pour optimiser le temps de recherche.

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

Durée exacte pour créer sparksession
| Méthode                          | Parquet   |  CSV     |
|----------------------------------|-----------|----------|
| Naive Notation (sans cache)      | 1.90512   | 7.5393   |
| Naive Notation (cache)           | 0.446794  | 4.55309  |
| Token Aware Notation (sans cache)| 1.98296   | 7.67941  |
| Token Aware Notation (cache)     | 0.452767  | 4.632418 |
|----------------------------------|-----------|----------|
| Spark Insult (sans cache)        | 1.989475  | 7.66823  |
| Spark Insult (cache)             | 0.482833  | 4.526515 |
| Map Reduce Insult (sans cache)   | 48.975855 | 55.54424 |
| Map Reduce Insult (cache)        | 45.427083 | 51.254813|

données calculées en moyenne sur 20 échantillons


ingest : 15.2977

On prend soin de vider autant que possible la mémoire pour ne pas crash pour les recommandations

global recsys 69.0930 (batch size = 10000, nombre de partition = 10)
user_specific recsys 627.6861 (batch size = 500, nombre de partition = 200)

En théorie, en réduisant le batch size et en augmentant le nombre de partition, on devrait obtenir une exécution plus rapide. Cependant, mon ordinateur n'est pas assez puissant pour faire tourner des simulations en augmentant le nombre de partition.


### Interprétations

On remarque d'abord, comme prévu, que les fichiers parquet sont bien plus optimisés pour un traitement rapide de la donnée que les fichiers csv, et ce dans tous les cas.

Ensuite, la méthode avec map reduce est bien plus lente que la méthode simple avec spark pour le traitement des insultes.

Enfin, pour la recommandation de jeux, la méthode personnalisée pour chaque utilisateur est bien plus couteuse que la méthode de recommandation basée sur la popularité des jeux, ce qui explique que cette dernière fonctionnalité est bien plus souvent utilisée.

Dans tous les cas, l'utilisation du cache rend l'exécution plus rapide, on en déduit que c'est une fonctionnalité utilisée de base par les fonctions utilisées, même si on ne le force pas, ce qui peut expliquer la proximité de résultats pour le classement par jeux entre l'approche naive et l'approche avec mémorisation des token.

## Pistes pour de futures avancées

- Optimisation des recommandations de jeux par utilisateurs
- Benchmark plus complet des fonctions de recommandation

#!/bin/bash

RES=../../resultat.txt
echo "Benchmarking insult_ranking.py" >> $RES

echo "spark" >> $RES
echo "parquet" >> $RES
for ((i=1; i<=20; i++)); do
    python insult_ranking.py parquet spark >> $RES
done

echo "csv" >> $RES
for ((i=1; i<=20; i++)); do
    python insult_ranking.py csv spark >> $RES
done

echo "map_reduce" >> $RES
echo "parquet" >> $RES
for ((i=1; i<=20; i++)); do
    python insult_ranking.py parquet map_reduce >> $RES
done

echo "csv" >> $RES
for ((i=1; i<=20; i++)); do
    python insult_ranking.py csv map_reduce >> $RES
done

echo "Benchmarking review_ranking.py" >> $RES

echo "token aware" >> $RES
echo "parquet" >> $RES
for ((i=1; i<=20; i++)); do
    python review_ranking.py parquet spark >> $RES
done

echo "csv" >> $RES
for ((i=1; i<=20; i++)); do
    python review_ranking.py csv spark >> $RES
done

echo "naive" >> $RES
echo "parquet" >> $RES
for ((i=1; i<=20; i++)); do
    python review_ranking.py parquet naive >> $RES
done

echo "csv" >> $RES
for ((i=1; i<=20; i++)); do
    python review_ranking.py csv naive >> $RES
done

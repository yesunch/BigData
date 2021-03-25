---
title: Projet Architecture BigData
---

# Projet Architecture BigData
Par Ivonne ROA RODRIGUEZ, Ruyi ZHENG, Ye SUN

## Context
### Data source
Nous avons utilisé la source de données offert par  [Pubnub](https://www.pubnub.com/developers/realtime-data-streams/financial-securities-market-orders/). C'est une source de données artificielles pour stimuler des ordres de marché d'action.

Son schema est :
```
{
    bid_price:212,
    order_quantity:739,
    symbol:'Bespin Gas',
    timestamp:12342343234,
    trade_type:"limit"
}
```
Le `bid_price` est le prix, `order_quantity`est le nombre d'action acheté, ici c'est un ordre de type `limit`. C'est à dire que c'est un ordre d'achat d'une action assorti d'une restriction sur le prix maximum à payer.
### Le traitement de données 

Dans la partie de Batch processing, nous avons calculé le somme de `order_quantity` par `trade_type`. Le temps de batch est 30 secondes. 


D'autre part, le traitement des données était un défi, car les données obtenus par streaming ne peuvent pas être traitées de la même manière que les données par batch. Le traitement des données s'est limité à trouver la moyenne de la variable *bidprice* pour chaque *symbol* dans une fenêtre temporelle de 10 secondes, en utilisant la fonction *withWatermark*. En général, cette fonction vous permet de définir une période de temps. Le résultat est le suivant.

![](https://i.imgur.com/QPkWPOW.png =400x)

## Architecture of the project

---
![Architecture](https://i.imgur.com/nAjbgsE.png)

---
## Import data into Kafka
D'abord, nous avons besoins de récupérer des données depuis l'API. Selon des instructions de Pubnub Developer, nous avons écrit un scipt python `get_data.py` et reussi à obtenir des données: 


![](https://i.imgur.com/5wtJFtj.png)

Et puis nous avons créé un Kafka Producer pour publier ces données à topic `market`. Pour tester, on utilise le pre-packaged Kafka consumer:

![](https://i.imgur.com/8Bl9LW5.png)



## Batch Processing
Notre batch processing est basée sur Map Reduce et Hadoop HDFS. L'objectif est de calculer le nombre total d’ordre par trade type pendant 30 secondes. Donc chaque 30 secondes, on va sauvegarder des données obtenues par `get_data.py` dans un fichier local, et le transmettre vers HDFS. Et puis lancer un MapReduce job sur Hadoop utilisant nos sript de mapper et reducer qui sont écris en python.

Dans `mapper.py`, les ordres sont transmets par stdin, nous récupérons le nombre d'ordre et le type de trade, les imprimons sur stdout. Ici la clé est le type de trade, le valeur est le nombre d'ordre. 

Et puis, `reducer.py` lit les donnés par stdin, convertie le nombre d'ordre à un int, et sommer les nombre d'ordre qui ont la même type de trade.

Le résultat est sauvegardé dans un fichier de HDFS, et il sera ensuite importé dans Cassandra.





## Stream Processing

Nous utilisons Spark Stream pour récupérer les données provenant du *topic* market de Kafka. Afin de les récupérer, nous définissons d'abord la connexion, où le paramètre *startingOffset* est le *latest* (pour qu'elle fonctionne en mode streaming et non batch). Ensuite, nous construisons le schéma comme indiqué :

```
marketSchema = StructType([\
    StructField("order_quantity", LongType()), StructField("trade_type", StringType()),\
    StructField("symbol", StringType()), StructField("timestamp", TimestampType()),\
    StructField("bid_price", FloatType())])
```

Afin de pouvoir analyser les données de Kafka vers pyspark, nous avons utilisé la fonction *from_json* du paquet *pyspark.sql.functions*, de cette façon nous avons obtenu chaque valeur du topic Kafka, qui à son tour a été mappé au schéma défini ci-dessus et nous avons obtenu seulement l'élément de colonne que nous voulions. Nous suivons ce processus avec toutes les colonnes définies dans le schéma, car ce sont les données que nous allons stocker dans Cassandra.

![](https://i.imgur.com/oQgt3UH.png =450x)



## Import data into Cassandra

Parce que Hadhoop enregistrera les résultats calculés dans le système de fichiers HDFS en format comme `part-00000`. Pour résoudre ce problème, nous avons préparé un Python script `hdfsToCassandra.py`, ce qui connec à HDFS pour lire le contenu du fiche.


`hdfsToCassandra.py` peut aussi connecter à Cassandra et créer la métaspace et la table pour stocker le résultat vient de HDFS fiche.

En conclusion, ce script travaile comme une pipeline reliant HDFS et Cassandra


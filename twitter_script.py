# -*- coding: utf-8 -*-
"""
Created on Tue Nov 16 16:05:11 2021

@author: Pierre Cottais & An Hoàng
"""

##################################################
# Chargement des modules
##################################################

import pandas as pd  # manipulation de dataframes
from pymongo import MongoClient  # utilisation de MongoDB avec Python
import zipfile
import time
from pprint import pprint  # affichage plus "joli"
# import json  # manuipulation de fichiers au format JSON
# import numpy as np  # manipulation de vecteurs
import random as rd
import matplotlib.pyplot as plt  # création de graphiques
from wordcloud import WordCloud  # création de nuages de mots
from datetime import datetime  # manipulation de données au format datetime
from pyspark import SparkConf, SparkContext  # utilisation de Spark avec Python
from pyspark.sql import SparkSession  # idem


##################################################
# Définition de fonctions
##################################################

# ajout du champ 'Opinion' (i.e. du libellé du sentiment)
def sentimentOpinion(liste):  # liste de dictionnaires
    for elem in liste:
        if elem["Sentiment"] == -1:
            elem["Opinion"] = "Ne croit pas au réchauffement climatique"
        elif elem["Sentiment"] == 0:
            elem["Opinion"] = "Sans opinion"
        elif elem["Sentiment"] == 1:
            elem["Opinion"] = "Croit au réchauffement climatique"
        else:
            elem["Opinion"] = "Relaie des faits d'acutalité à propos du réchauffement climatique"


# création et enregistrement d'un nuage de mots
def plotWordCloud(dataframe):
    words = list(dataframe["Hashtag"])
    freq = list(dataframe["Frequency"])
    words_dict = dict(zip(words, freq))
    title = dataframe["Opinion"].values[0]
    cloud = WordCloud(width=800, height=400,
                      background_color="white",
                      max_words=50, min_font_size=10).generate_from_frequencies(words_dict)
    plt.imshow(cloud)
    plt.axis("off")
    plt.title(title)
    plt.tight_layout(pad=0)
    plt.savefig('figures/wordcloud'+str(dataframe["Sentiment"].values[0])+'.png')

##################################################
# Traitement des données (nettoyage et fusion)
##################################################
print("====================")
print("TRAITEMENT DES DONNÉES")
print("====================")

# connexion à la base de données du serveur local de MongoDB
client = MongoClient(host="127.0.0.1:27017")
db = client["mydb"] # choix de la base de données
# print(client)
# print(db)
print("Collections contenues dans la base mydb :",
      db.list_collection_names())

# décompression du fichier ZIP contenant les données

# try:
#     with zipfile.ZipFile("data/dfiles.zip") as z:
#         z.extractall("data/")
#         print("Fichiers décompressés")
# except:
#     print("Fichier compressé incorrect")

# chargement du fichier contenant les "sentiments" sur le changement climatique
sentiment = pd.read_csv(r'data/twitter_sentiment_data.csv')

# extraction des l'identifiant des tweets générés avec 'Hydration' de Twitter
sentiment['tweetid'].to_csv(r'data/ID.txt', 
                            header=None, index=None, sep = ' ', mode= 'a')

# chargement de la collection 'twitter'
tw = db.twitter

# ajout d'un champ 'sentiment' pour chaque document de la collection
#  par correspondance avec l'ID dans le fichier twitter_sentiment_data.csv
if "sentiment" in tw.find_one().keys():  # sélection du permier tweet pour vérifier
    print("Le champ 'sentiment' existe déjà dans la base.")
else:
    tweets = list(tw.find())
    # number = tw.count()  # obsolète
    number = tw.count_documents({})
    for i in range(0,number):
        a = int(tweets[i]["id_str"]),
        b = sentiment.loc[(sentiment['tweetid']==a)],
        tw.update_one(
            {"_id": tweets[i]["_id"]},
            { "$set": {"sentiment": int(b[0]['sentiment']) }}
        )
        print("La collection a été modifiée (ajout du champ 'sentiment').")

  # sélection du permier tweet pour vérifier l'existence
# if "sentiment" in find_one.keys():
#     print("Le champ 'sentiment' existe déjà dans la base.")
# else:
#     print("Le champ 'sentiment' n'est pas dans la base.")
#     dico_sentiment = {}
#     for i in range(0, len(sentiment)):
#         dico_sentiment[sentiment["tweetid"][i]] = sentiment["sentiment"][i]
    
#     liste_id = list(sentiment["tweetid"].values)
#     liste_id = list(map(str, liste_id))
    
#     res = tw.update_many(
#         {"_id": {"$in": liste_id}},
#         [{"$set": {"sentiment": 12}}]
#     )
    
#     for doc in tw.find({"_id": {"$in": liste_id}}):
#         print(doc)
    
#     for doc in res:
#         print(doc)
    
#     if "sentiment" in tw.find_one().keys():
#         print("La collection a été modifiée (ajout du champ 'sentiment').")

    
# vérification si le sentiment est mis à jour par sélection aléatoire d'un tweet
# check = list(tw.find())
# random = rd.choice(range(0, number))
# print("Sentiment value of tweet", random +1 ,"= {}\n".format(check[random]["sentiment"]))


# ajout d'un champ 'date'
if "date" in tw.find_one().keys():
    print("Le champ 'date' existe déjà dans la base.")
else:
    tw.update_many({}, [
        {"$set": {
            "date": {"$dateFromString": 
                     {"dateString": {"$concat": [
                         {"$substr": ["$created_at", 0, 11]},
                         {"$substr": ["$created_at", 26, 30]}
                     ]}}
            }
        }}
    ])
    print("La collection a été modifiée (ajout du champ 'date').")


##################################################
# Première partie : analyse exploratoire
##################################################
print("====================")
print("0. Premières requêtes de test")
print("====================")

# comptage du nombre de tweets
# number = tw.count()  # obsolète
number = tw.count_documents({})  # 27347
print(number, "tweets (soit 62% ayant une correspondance par ID)")

print("> Combien de tweets pour chaque langue ?")
language = tw.aggregate([
            { "$group": {
                "_id": '$lang',
                "count": {"$sum": 1}
            }}
])
pprint(list(language))

# Autre façon de faire une requête : décomposition par étapes
# tweets en anglais
query = {"lang": "en"}  # contenu de la requête
req1 = tw.find_one(query)  # premier tweet anglais
req =  list(tw.find(query))  # liste de tous les tweets anglais
print("Contenu du premier tweet en anglais :\n", req[0]["full_text"])
print("Et son identifiant :", req[0]["user"]["id_str"])

# affichage de plusieurs tweets
print("Affichage des 3 premiers tweets :\n")
for i in range(0,3):
    print("Tweet", i+1 ,"content = {}\n".format(req[i]["full_text"]))



print("====================")
print("1. Nombre de tweets pour chaque 'sentiment'")
print("====================")

sentiment = tw.aggregate([
            { "$group": {
                "_id": '$sentiment',
                "count": {"$sum": 1}
            }},
            {"$sort": {
                "count": -1         # tri des tweets par ordre décroissant
            }}
])

# -1: don't believe in climate change (1742)
#  0: neutre opinion (4425)
#  1: believe in climate change (14840)
#  2: report news about climate change (6340)

df = pd.DataFrame(list(sentiment))
print(df)
df["_id"][df["_id"]==-1] = "Ne croit pas au réchauffement climatique"
df["_id"][df["_id"]==0] = "Sans opinion"
df["_id"][df["_id"]==1] = "Croit au réchauffement climatique"
df["_id"][df["_id"]==2] = "Relaie des faits d'acutalité à propos du réchauffement climatique"

# création du graphique
ax = df.plot.barh(y = 'count', x = '_id', legend = False, color = "#1DA1F3")
ax.set_xlabel("Nombre de tweets")
ax.set_ylabel("")

# enregistrement du graphique dans un dossier dédié aux figures
ax.figure.savefig('figures/barplot_sentiment.png',
                  dpi = 200, bbox_inches = "tight")
fig = ax.get_figure()
plt.close(fig)  # ferme la fnêtre graphique (très important pour ceux d'après)

print("====================")
print("2. Hashtags les plus populaires pour chaque 'sentiment'")
print("====================")

# hashtags = tw.find({"entities.hashtags": {"$ne": []}},
#                       {"entities.hashtags.text": 1})
# hashtags = tw.find({"entities.hashtags": {"$exists": True, "$not": {"$size": 0}}},  # tweets containing hashtags
#                       {"sentiment":1, "entities.hashtags.text": 1})
top_hashtags = tw.aggregate([
    {"$unwind": "$entities.hashtags"},
    {"$group" : {"_id":{"sentiment":"$sentiment", "hashtag": "$entities.hashtags.text"},
                 "Nb#":{"$sum": 1}}},
    {"$sort": {"Nb#": -1}}
])

# construction du top 10 des hashtags par sentiment
hashtag_list = []
top_dict = {-1: 0, 0: 0, 1: 0, 2: 0}  # compteur pour le classement (top 10)
for doc in top_hashtags:
    # pprint(doc)
    if top_dict[doc["_id"]["sentiment"]] < 10:
        hashtag_list.append({"Frequency": doc["Nb#"],
                            "Hashtag": doc["_id"]["hashtag"],
                            "Sentiment": doc["_id"]["sentiment"]})   
        top_dict[doc["_id"]["sentiment"]] += 1
# pprint(hashtag_list)

sentimentOpinion(hashtag_list)  # ajout de l'opinion (libellé du 'sentiment')
df = pd.DataFrame(hashtag_list)

# itération pour chaque 'sentiment'
for s in range(-1, 3):
    plotWordCloud(df[df["Sentiment"]==s])
    
print("====================")
print("3. Évolution du 'sentiment' des tweets dans le temps")
print("====================")
print("3.1.  Série temporelle par mois")
print("==========")

# comptage du nombre de tweets par mois selon le 'sentiment'
month_aggreg = tw.aggregate([
    {"$group": {"_id": {"Mois": {"$month": "$date"},
                        "Année": {"$year": "$date"},
                        "Sentiment": "$sentiment"},
                "nb": {"$sum": 1}}
    },
    {"$sort": {"_id.Année": 1, "_id.Mois": 1}}
])  # différenciation du mois selon l'année et tri par ordre chronologique

# traitement de l'aggrégat pour générer un jeu de données
monthseries = []
for doc in list(month_aggreg):
    # pprint(doc)
    monthseries.append({"Date": datetime.strptime(str(doc["_id"]["Année"])+"-"+
                                                 str(doc["_id"]["Mois"]), "%Y-%m"),
                       "Sentiment": doc["_id"]["Sentiment"],
                       "Nb": doc["nb"]})
# sentimentOpinion(monthseries)  # ajout de l'opinion (libellé du sentiment)
df = pd.DataFrame(monthseries)

# modification du jeu de données afin de faciliter la création du grahpique
dt = pd.pivot(df, index = "Date", columns = "Sentiment", values = "Nb")
# print(dt)
dt.plot(subplots=True, linewidth = 2)
plt.tight_layout(pad=0)
plt.savefig('figures/timeseries_month.png', dpi = 200)


print("3.2.  Série temporelle par jour")
print("==========")

# idem pour le détail par jour

day_aggreg = tw.aggregate([
    {"$group": {"_id": {"Date": "$date",
                        "Sentiment": "$sentiment"},
                "nb": {"$sum": 1}}
    },
    {"$sort": {"_id.Date": 1}}
])

dayseries = []
for doc in list(day_aggreg):
    # pprint(doc)
    dayseries.append({"Date": doc["_id"]["Date"],
                       "Sentiment": doc["_id"]["Sentiment"],
                       "Nb": doc["nb"]})

# sentimentOpinion(dayseries)
df = pd.DataFrame(dayseries)

dt = pd.pivot(df, index = "Date", columns = "Sentiment", values = "Nb")
# print(dt)
dt.plot(subplots=True, linewidth = 1)
plt.legend(loc = "upper right")
plt.tight_layout(pad=0)
plt.savefig('figures/timeseries_day.png', dpi = 200)


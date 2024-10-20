

from pyspark.sql import SparkSession
from datetime import datetime
import math
from django.shortcuts import render, redirect

def verifier_regle_verticale(file_name):
    # Start Spark Session
    spark = SparkSession.builder.appName("ETAT COMPTABLE")\
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1')\
            .getOrCreate()
    sqlContext = SparkSession(spark)
    
    # Don't Show warning only error
    spark.sparkContext.setLogLevel("ERROR")

    mongodf = spark.read.format("mongo") \
        .option("uri", "mongodb://localhost:27017/") \
        .option("database", "db_etat_comptable") \
        .option("collection", file_name) \
        .load()

    mongodf.createOrReplaceTempView("tempMongo")

    newdf = sqlContext.sql("SELECT code_etablissment,date,codedoc,codemon,etat,data FROM tempMongo")

    row = newdf.first().asDict()
    formatted_element = {
        "_id": {"$oid": "663ff4c1f9c2dc9f6bc03065"},
        "code_etablissment": row['code_etablissment'],
        "date": row['date'],
         "codedoc": row['codedoc'],
        "codemon": row['codemon'],
        "etat": row['etat'],
        "data": row['data']
    }

    codedocrep = ""
    dictionnaire = {}
    # Initialisation de l'ensemble
    etat = set()
    sorted_df = newdf.orderBy("codedoc")

    for row in sorted_df.collect():
        formatted_element = {
            "_id": {"$oid": "663ff4c1f9c2dc9f6bc03065"},
            "code_etablissment": row['code_etablissment'],
            "date": row['date'],
            "codedoc": row['codedoc'],
            "codemon": row['codemon'],
            "etat": row['etat'],
            "data": row['data']
        }
        keys_to_display = ["code_etablissment", "date", "codedoc", "codemon", "etat"]
        data = formatted_element['data']
          
        if codedocrep != formatted_element.get("codedoc"):
            codedoc=formatted_element.get("codedoc")
            #print(codedoc)
            etat.add(codedoc)
            codedocrep = formatted_element.get("codedoc")
          
        colonnes_a_ne_pas_afficher = ["code", "num ligne"]
        debut=""
        fincodemon=fincode=finrang=finvalue=""
        for row_data in data:
            for column, value in row_data.asDict().items():
                if value is not None and column not in colonnes_a_ne_pas_afficher:
                    fin=debut
                    debut=formatted_element.get("codemon") +"   "+row_data["code"]
                    if(debut!=fin):
                        #print(fincodemon+"   "+fincode+"   "+finrang+"   "+str(finvalue))
                        try:
                            dictionnaire[(fincodemon,fincode,codedoc)]=float(finvalue)
                        except ValueError:
                            #cas de erreur on stocke 0
                            dictionnaire[(fincodemon,fincode,codedoc)]=0
                    fincodemon=formatted_element.get("codemon")
                    fincode=row_data["code"]
                    finrang=column
                    finvalue=value
                    
    return dictionnaire,etat

import re

def extract_operator(expression):
    operators = ['<=', '>=', '<', '>', '=']
    for op in operators:
        if op in expression:
            return op
    return None

def compare_values(a, b, operator):
    # Perform the comparison
    if operator == '=':
        result = a == b
    elif operator == '<':
        result = a < b
    elif operator == '>':
        result = a > b
    elif operator == '<=':
        result = a <= b
    elif operator == '>=':
        result = a >= b
    
    return result

def parse_rule(rule, splitby, doc, dictionnaire):
    # Extract left and right sides of the rule
    left, right = rule.split(splitby)

    # Extract values from left side
    left_values = [re.search(r"(\w+)\(mon:(\d+)\)", val).group(1) for val in left.split(" + ")]
    left_mon = [int(re.search(r"(\w+)\(mon:(\d+)\)", val).group(2)) for val in left.split(" + ")]

    # Extract values from right side
    right_values = [re.search(r"(\w+)\(mon:(\d+)\)", val).group(1) for val in right.split(" + ")]
    right_mon = [int(re.search(r"(\w+)\(mon:(\d+)\)", val).group(2)) for val in right.split(" + ")]

#     print("Left values:", left_values)
#     print("Left vars:", left_mon)
#     print("Right values:", right_values)
#     print("Right vars:", right_mon)

    # Calculate sum for the right side
    sommeleft = 0
    for element1, element2 in zip(left_values, left_mon):
        try:
            value = dictionnaire[(str(element2), element1, doc)]
#             print(element1, element2)
#             print(value)
            sommeleft += value

        except KeyError:
#             print(f"Key ({str(element2)}, {element1}, {doc}) not found in dictionary")
            return
#     print("somme : " + str(sommeright))
#     print("//////////////")

    # Calculate sum for the left side
    sommeright = 0
    for element1, element2 in zip(right_values, right_mon):
        try:
            value = dictionnaire[(str(element2), element1, doc)]
#             print(value)
            sommeright += value
        except KeyError:
#             print(f"Key ({str(element2)}, {element1}, {doc}) not found in dictionary")
            return
#     print("somme : " + str(sommegauch))

#     print("//////////////")

    result=compare_values(sommeleft,sommeright, splitby)
    
    if result:
#         print("somme1 : " + str(sommeleft)+" "+splitby+" somme2 : " + str(sommeright))
#         print(rule)
#         print("yes")
        return True
    else:
#         print("no")
        return False

def verifier_resultat_regle_verti(rule,cle_recherchee,dictionnaire,vrairule,fausserule): 
    operator = extract_operator(rule)
    if operator:
#         print(operator)
        
        if parse_rule(rule, operator, cle_recherchee, dictionnaire):
            vrairule.append(rule)
        else :
            fausserule.append(rule)
    else:
        print("No valid operator found in the string")


import json
from pymongo import MongoClient
from bson.objectid import ObjectId

def FonctionPrincipaleRegleVerticale(col):
    # Connexion à la base de données MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['db_etat_comptable']
    collection = db['regles']

    # Initialisation du dictionnaire pour stocker les règles verticales
    regles_verticales = {}
    vrairule= []
    fausserule= []

    # Récupération des règles verticales depuis la base de données
    for document in collection.find({"REGLES_VERTICALES": {"$exists": True}}):
        regle_id = str(document['_id'])
        regle_v = document['REGLES_VERTICALES']
        regle = document['Regle']

        # Si la clé REGLES_VERTICALES n'existe pas dans le dictionnaire, l'initialiser avec une liste vide
        if regle_v not in regles_verticales:
            regles_verticales[regle_v] = []

        # Ajouter la règle à la liste correspondante dans le dictionnaire
        regles_verticales[regle_v].append(regle)

    # # Afficher les règles verticales stockées dans le dictionnaire
    # for cle, valeurs in regles_verticales.items():
    #     print(f"Cle: {cle}")
    #     print("Liste des règles:")
    #     for valeur in valeurs:
    #         print(valeur)
    #     print()
    dictionnaire,etat=verifier_regle_verticale(col)
    print("Les règles verticales ont été stockées avec succès dans un dictionnaire !")

    # cle_recherchee = "001"

    for cle_recherchee in sorted(etat):
        print(cle_recherchee)
        # Si la clé existe dans le dictionnaire, afficher la liste des règles correspondantes
        if cle_recherchee in regles_verticales:
            print(f"Liste des règles pour la clé {cle_recherchee}:")
            for regle in regles_verticales[cle_recherchee]:
#                 print(regle)
                verifier_resultat_regle_verti(regle,cle_recherchee,dictionnaire,vrairule,fausserule)
        else:
            print(f"Aucune règle trouvée pour la clé {cle_recherchee}.")
    return vrairule,fausserule
            
# def postRegleVerticale(request):
#     if request.method == 'POST':
#         collec = request.POST['collection_name']
#         vrairule,fausserule=FonctionPrincipaleRegleVerticale(collec)
#         return render(request, 'Verticale.html', {'vrairule': vrairule, 'fausserule': fausserule})
    
from django.shortcuts import render
from django.http import HttpRequest, HttpResponse

def postRegleVerticale(request: HttpRequest) -> HttpResponse:
    if 'username' not in request.session:
        return redirect('login')
    if request.method == 'POST':
        collec = request.POST['collection_name']
        vrairule, fausserule = FonctionPrincipaleRegleVerticale(collec)
        return render(request, 'ResultRegle.html', {'vrairule': vrairule, 'fausserule': fausserule,'Type':"VERTICALE"})
    else:
        return redirect('Verticale')

# Assurez-vous d'importer FonctionPrincipaleRegleVerticale et les autres dépendances nécessaires.


#///////////////////////////////////////////////////////////////////////////////////////////////////////////////
#///////////////////////////////////////Regle horizontale//////////////////////////////////////////////////////
def regeleHorizontale():
    
    client = MongoClient("mongodb://localhost:27017/")
    db = client['db_etat_comptable']
    collection = db['regles']
    # Initialisation du dictionnaire pour stocker les règles horizontales
    regles_horizontales = {}

    # Récupération des règles horizontales depuis la base de données
    for document in collection.find({"REGLES HORIZONTALES": {"$exists": True}}):
        regle_id = str(document['_id'])
        regle_h = document['REGLES HORIZONTALES']
        regle = document['Regle']

        # Si la clé REGLES HORIZONTALES n'existe pas dans le dictionnaire, l'initialiser avec une liste vide
        if regle_h not in regles_horizontales:
            regles_horizontales[regle_h] = []

        # Ajouter la règle à la liste correspondante dans le dictionnaire
        regles_horizontales[regle_h].append(regle)
    return regles_horizontales

def verify_rule(rule, values):
    # Extracting the left and right sides of the rule
    left_side, right_side = rule.split('=')
    
    # Extracting indices from the left and right sides
    left_indices = [int(num) - 1 for num in re.findall(r'\d+', left_side)]  # Adjusting indices to start from zero
    right_indices = [int(num) - 1 for num in re.findall(r'\d+', right_side)]  # Adjusting indices to start from zero
    
    # Check if any index is out of range
    max_index = len(values) - 1
    if any(index > max_index for index in left_indices + right_indices):
        print("Error: One or more indices are out of range.")
        return
    
    # Function to clean and convert values to float
    def clean_and_convert(value):
        try:
            return float(value)
        except ValueError:
            return 0.0

    # Getting values from the indices in the array and converting them to numbers
    left_values = [clean_and_convert(values[index]) for index in left_indices]  # Convert values to float
    right_values = [clean_and_convert(values[index]) for index in right_indices]  # Convert values to float

    # Extracting operator from the rule
    operator = extract_operator(rule)

    # Calculating the sum of the values
    left_sum = sum(left_values)
    right_sum = sum(right_values)

    # Printing the sums for reference
#     print("Sum of values on the left side:", left_sum)
#     print("Sum of values on the right side:", right_sum)

#     # Verifying the rule
#     print("Rule verification result:", end=" ")
    return compare_values(left_sum, right_sum, operator)

def verifier_regle_hori(file_name):
    regles_horizontales = regeleHorizontale()
    spark = SparkSession.builder.appName("ETAT COMPTABLE") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .getOrCreate()
    sqlContext = SparkSession(spark)
    spark.sparkContext.setLogLevel("ERROR")

    mongodf = spark.read.format("mongo") \
        .option("uri", "mongodb://localhost:27017/") \
        .option("database", "db_etat_comptable") \
        .option("collection", file_name) \
        .load()

    mongodf.createOrReplaceTempView("tempMongo")

    newdf = sqlContext.sql("SELECT code_etablissment,date,codedoc,codemon,etat,data FROM tempMongo")

    codedocrep = ""

    sorted_df = newdf.orderBy("codedoc")
    fausseregle = []
    vrairegle = []
    for row in sorted_df.collect():
        formatted_element = {
            "_id": {"$oid": "663ff4c1f9c2dc9f6bc03065"},
            "code_etablissment": row['code_etablissment'],
            "date": row['date'],
            "codedoc": row['codedoc'],
            "codemon": row['codemon'],
            "etat": row['etat'],
            "data": row['data']
        }
        keys_to_display = ["code_etablissment", "date", "codedoc", "codemon", "etat"]
        data = formatted_element['data']

        if codedocrep != formatted_element.get("codedoc"):
            codedoc = formatted_element.get("codedoc")
            codedocrep = formatted_element.get("codedoc")

            if codedoc in regles_horizontales:
                regle = regles_horizontales[codedoc]
            else:
                print(f"No rule found for codedoc: {codedoc}")
                continue

        colonnes_a_ne_pas_afficher = ["code", "num ligne"]
        for row_data in data:
            tableau = []
            for column, value in row_data.asDict().items():
                if value is not None and column not in colonnes_a_ne_pas_afficher:
                    try:
                        tableau.append(value)
                    except ValueError:
                        tableau.append(0)
            if verify_rule(regle[0], tableau):
                vrairegle.append("DOCUMENT: " + codedoc + " CODE MONNAIS :" + formatted_element.get(
                    "codemon") + " Code LIGNE : " + row_data["code"] + " " + regle[0])
            else:
                fausseregle.append("DOCUMENT: " + codedoc + " CODE MONNAIS :" + formatted_element.get(
                    "codemon") + " Code LIGNE : " + row_data["code"] + " " + regle[0])
    return fausseregle, vrairegle


# Call the main function


def postRegleHorizontale(request: HttpRequest) -> HttpResponse:
    if 'username' not in request.session:
        return redirect('login')
    if request.method == 'POST':
        collec = request.POST['collection_name']
        fausserule,vrairule =verifier_regle_hori("01/2024")
        return render(request, 'ResultRegle.html', {'vrairule': vrairule, 'fausserule': fausserule,'Type':"HORIZONTALE"})
    else:
        return redirect('Horizontale')

# Assurez-vous d'importer FonctionPrincipaleRegleVerticale et les autres dépendances nécessaires.



#/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#///////////////////////////////////////Regle Inter Document//////////////////////////////////////////////////////

from pyspark.sql import SparkSession
from datetime import datetime
import math

def verifier_regle_interDocumnet(file_name):
    # Start Spark Session
    spark = SparkSession.builder.appName("ETAT COMPTABLE")\
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1')\
            .getOrCreate()
    sqlContext = SparkSession(spark)
    
    # Don't Show warning only error
    spark.sparkContext.setLogLevel("ERROR")

    mongodf = spark.read.format("mongo") \
        .option("uri", "mongodb://localhost:27017/") \
        .option("database", "db_etat_comptable") \
        .option("collection", file_name) \
        .load()

    mongodf.createOrReplaceTempView("tempMongo")

    newdf = sqlContext.sql("SELECT code_etablissment,date,codedoc,codemon,etat,data FROM tempMongo")

    row = newdf.first().asDict()
    formatted_element = {
        "_id": {"$oid": "663ff4c1f9c2dc9f6bc03065"},
        "code_etablissment": row['code_etablissment'],
        "date": row['date'],
         "codedoc": row['codedoc'],
        "codemon": row['codemon'],
        "etat": row['etat'],
        "data": row['data']
    }

    codedocrep = ""
    dictionnaire = {}
    # Initialisation de l'ensemble
    etat = set()
    sorted_df = newdf.orderBy("codedoc")

    for row in sorted_df.collect():
        formatted_element = {
            "_id": {"$oid": "663ff4c1f9c2dc9f6bc03065"},
            "code_etablissment": row['code_etablissment'],
            "date": row['date'],
            "codedoc": row['codedoc'],
            "codemon": row['codemon'],
            "etat": row['etat'],
            "data": row['data']
        }
        keys_to_display = ["code_etablissment", "date", "codedoc", "codemon", "etat"]
        data = formatted_element['data']
          
        if codedocrep != formatted_element.get("codedoc"):
            codedoc=formatted_element.get("codedoc")
            #print(codedoc)
            etat.add(codedoc)
            codedocrep = formatted_element.get("codedoc")
          
        colonnes_a_ne_pas_afficher = ["code", "num ligne"]
        for row_data in data:
            for column, value in row_data.asDict().items():
                if value is not None and column not in colonnes_a_ne_pas_afficher:
                    fincodemon=formatted_element.get("codemon")
                    fincode=row_data["code"]
                    finrang=column
                    finvalue=value
                    try:
                        dictionnaire[(fincodemon,fincode,codedoc,finrang)]=float(finvalue)
                    except ValueError:
                        dictionnaire[(fincodemon,fincode,codedoc)]=0
                    
    return dictionnaire,etat


import re

def verifier_resultat_regle_inter(rule, cle_recherchee):
    operator = extract_operator(rule)
    if operator:
        print(operator)
        parse_rule_table_complex_separate(rule)
    else:
        print("No valid operator found in the string")


def parse_rule_table_complex_separate(rule,splitby,dictionnaire_inter):
    # Split the rule into left and right sides
    left, right = rule.split(splitby)

    # Extract information from the left side
    left_tables = re.findall(r'\w+\[Doc: \d+\]\[mon:\d+\]\[Col: \d+\]', left)
    left_info = {"table": [], "doc": [], "mon": [], "col": []}

    for table in left_tables:
        left_info["table"].append(table.split("[")[0])
        left_info["doc"].append(table.split("[")[1].split(":")[1].split("]")[0])
        left_info["mon"].append(table.split("[")[2].split(":")[1].split("]")[0])
        left_info["col"].append(table.split("[")[3].split(":")[1].split("]")[0])

    # Extract information from the right side
    right_tables = re.findall(r'\w+\[Doc: \d+\]\[mon:\d+\]\[Col: \d+\]', right)
    right_info = {"table": [], "doc": [], "mon": [], "col": []}

    for table in right_tables:
        right_info["table"].append(table.split("[")[0])
        right_info["doc"].append(table.split("[")[1].split(":")[1].split("]")[0])
        right_info["mon"].append(table.split("[")[2].split(":")[1].split("]")[0])
        right_info["col"].append(table.split("[")[3].split(":")[1].split("]")[0])

    # Calculate sum for the right side
    sommeleft = 0
    for element1, element2, element3, element4 in zip(left_info["table"], left_info["doc"], left_info["mon"],
                                                        left_info["col"]):
        try:
            value = dictionnaire_inter[(str(element3).strip(), element1.strip(), element2.strip(), str(element4).strip())]

            sommeleft += value

        except KeyError:
#             print(f"Key ({str(element3)}, {element1}, {element2},{str(element4)}) not found in dictionary")
            return

    # Calculate sum for the left side
    sommeright = 0
    for element1, element2, element3, element4 in zip(right_info["table"], right_info["doc"], right_info["mon"],
                                                     right_info["col"]):
        try:
            value = dictionnaire_inter[(str(element3).strip(), element1.strip(), element2.strip(), str(element4).strip())]
            sommeright += value
        except KeyError:
#             print(f"Key ({str(element3)}, {element1}, {element2},{str(element4)}) not found in dictionary")
            return

#     print("somme1 : " + str(sommeright) + " " + splitby + " somme2 : " + str(sommeleft))
    
    return compare_values(sommeleft, sommeright, splitby)


# Test the function with the given rule

def verifier_resultat_regle_inter(rule, cle_recherchee,dictionnaire_inter,truerule,falserule):
    operator = extract_operator(rule)
    if operator:
#         print(operator)
        if parse_rule_table_complex_separate(rule,operator,dictionnaire_inter):
            truerule.append(rule)
            print("yes")
        else:
            falserule.append(rule)
            print("no")
    else:
        print("No valid operator found in the string")



import json
from pymongo import MongoClient
from bson.objectid import ObjectId

def FonctionPrincipaleRegleInterDocument(col):
    # Connexion à la base de données MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['db_etat_comptable']
    collection = db['regles']

    # Initialisation du dictionnaire pour stocker les règles interdocuments
    regles_interdocuments = {}

    # Récupération des règles interdocuments depuis la base de données
    for document in collection.find({"REGLES INTERDOCUMENTS": {"$exists": True}}):
        regle_id = str(document['_id'])
        regle_i = document['REGLES INTERDOCUMENTS']
        regle = document['Regle']

        # Si la clé REGLES INTERDOCUMENTS n'existe pas dans le dictionnaire, l'initialiser avec une liste vide
        if regle_i not in regles_interdocuments:
            regles_interdocuments[regle_i] = []

        # Ajouter la règle à la liste correspondante dans le dictionnaire
        regles_interdocuments[regle_i].append(regle)

    dictionnaire_inter, etat = verifier_regle_interDocumnet(col)
    truerule = []
    falserule = []

    for cle_recherchee in sorted(etat):
        print(cle_recherchee)
        # Si la clé existe dans le dictionnaire, afficher la liste des règles correspondantes
        if cle_recherchee in regles_interdocuments:
            print(f"Liste des règles pour la clé {cle_recherchee}:")
            for regle in regles_interdocuments[cle_recherchee]:
                verifier_resultat_regle_inter(regle, cle_recherchee, dictionnaire_inter, truerule, falserule)
        else:
            print(f"Aucune règle trouvée pour la clé {cle_recherchee}.")
    return truerule, falserule
            

def postRegleInterDocument(request: HttpRequest) -> HttpResponse:
    if 'username' not in request.session:
        return redirect('login')
    if request.method == 'POST':
        collec = request.POST['collection_name']
        vrairule,fausserule =FonctionPrincipaleRegleInterDocument("01/2024")
        return render(request, 'ResultRegle.html', {'vrairule': vrairule, 'fausserule': fausserule,'Type':"INTER DOCUMENT"})
    else:
        return redirect('InterDocument')
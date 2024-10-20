import mimetypes
import os
import time
import concurrent.futures
import pandas as pd
import re
import uuid  # Module pour générer des identifiants uniques
from django.shortcuts import render
from django.http import HttpResponse, FileResponse
from pymongo import MongoClient
from IPython.display import HTML
from datetime import datetime
from pyspark.sql import SparkSession
from django.shortcuts import get_object_or_404
from django.conf import settings
from django.shortcuts import render, redirect


from django.views.decorators.csrf import csrf_exempt

def remise_view(request):
    if 'username' not in request.session:
        return redirect('login')
    return render(request, 'Remise.html')
def home_view(request):
    if 'username' not in request.session:
        return redirect('login')
    return render(request, 'home.html')
def ComptreRendu_view(request):
    if 'username' not in request.session:
        return redirect('login')
    return render(request, 'ComptreRendu.html')
def Verticale_view(request):
    if 'username' not in request.session:
        return redirect('login')
    # Connexion à MongoDB
    client = MongoClient('localhost', 27017)
    db = client['db_etat_comptable']
    
    # Récupérer toutes les collections existantes
    all_collections = db.list_collection_names()
    
    # Filtrer les collections pour celles qui correspondent au format "??/????"
    collections = [collection for collection in all_collections if re.match(r'\d{2}/\d{4}', collection)]
    return render(request, 'Verticale.html', {'collections': collections})  # Utiliser 'collections' au lieu de 'collection'


def Horizontale_view(request):
    # Connexion à MongoDB
    client = MongoClient('localhost', 27017)
    db = client['db_etat_comptable']
    
    # Récupérer toutes les collections existantes
    all_collections = db.list_collection_names()
    
    # Filtrer les collections pour celles qui correspondent au format "??/????"
    collections = [collection for collection in all_collections if re.match(r'\d{2}/\d{4}', collection)]
    return render(request, 'Horizontale.html', {'collections': collections})  # Utiliser 'collections' au lieu de 'collection'
def InterDocument_view(request):
   # Connexion à MongoDB
    client = MongoClient('localhost', 27017)
    db = client['db_etat_comptable']
    
    # Récupérer toutes les collections existantes
    all_collections = db.list_collection_names()
    
    # Filtrer les collections pour celles qui correspondent au format "??/????"
    collections = [collection for collection in all_collections if re.match(r'\d{2}/\d{4}', collection)]
    return render(request, 'InterDocument.html', {'collections': collections})  # Utiliser 'collections' au lieu de 'collection'
#/////////////////////////////////////////////CATEGORIES/////////////////////////////////////////////////////

from django.shortcuts import render
from pymongo import MongoClient

def load_existing_categories(request):
    try:
        client = MongoClient('localhost', 27017)
    except Exception as e:
        return render(request, 'Categories.html', {'categories': [], 'error': 'Failed to connect to MongoDB'})
    
    db = client['db_etat_comptable']
    collection = db['categories']
    
    try:
        existing_categories = list(collection.find({}, {"_id": 0, "code": 1, "description": 1}))
    except Exception as e:
        return render(request, 'Categories.html', {'categories': [], 'error': 'Failed to fetch categories'})
    
    if not existing_categories:
        return render(request, 'Categories.html', {'categories': [], 'error': 'No categories found'})
    
    return render(request, 'Categories.html', {'categories': existing_categories})

# views.py

def load_existing_etat(request):
    # Connexion à MongoDB
    client = MongoClient('localhost', 27017)
    db = client['db_etat_comptable']
    
    # Récupérer toutes les collections existantes
    all_collections = db.list_collection_names()
    
    # Filtrer les collections pour celles qui correspondent au format "??/????"
    collections = [collection for collection in all_collections if re.match(r'\d{2}/\d{4}', collection)]
    
    return render(request, 'Etat.html', {'collections': collections})

#//////////////////////////////////////////////DEBUT TRAITEMENT///////////////////////////////////////////////////////
#///////////////////////////////////////////////////////////////////////////////////////////////////////////////
def extract_values_with_pipe(data, start_row):
    # Initialize an empty list to store non-NaN values containing '|'
    non_null_values_with_pipe = []
    compteur = start_row
    end_of_file = False
    # Définition de la phrase exacte à rechercher
    phrase_exacte = "Code Ligne"
    #print("coordonne")
    #print(start_row)
    #print(len(data) - 1)
    
    if start_row >=len(data) - 1:
        end_of_file = True
        return "", "", end_of_file
    
    # Iterate over each row
    for index, row in data.iloc[start_row:].iterrows():            
        # Check if the first cell value starts with "Code ligne"
        if re.search(re.escape(phrase_exacte), str(row.iloc[0]), re.IGNORECASE) or re.search(re.escape(phrase_exacte), str(row.iloc[1]), re.IGNORECASE):
            break  # Exit the loop if condition met
        compteur += 1
            
        # Iterate over each cell in the row
        for value in row:
            # Check if the value is not NaN and contains '|'
            if pd.notna(value) and '|' in str(value):
                non_null_values_with_pipe.append(value)
    #print(end_of_file)
    return non_null_values_with_pipe, compteur, end_of_file


# Function to extract numbers from a string
def extract_numbers_from_string(string):
    numbers = []
    for char in string:
        if char.isdigit():
            numbers.append(int(char))
    return numbers

# Function to fill variables    
def recuperer_entete_document(data, start_row,sheet_name):
    non_null_values_with_pipe, compteur, end_of_file = extract_values_with_pipe(data, start_row)
    if end_of_file:
        return "", "", "", "", "", end_of_file
    
    # List to store extracted numbers from all strings
    all_numbers = []
    code_etablissement = "012"    
    date = ""
    codedoc = ""
    codemon = ""
    if sheet_name.lower() in ["actif", "passif", "hors bilan"]:
        codedoc="001"
        codemon = "3"
    

    # Iterate over each string in the list
    for string in non_null_values_with_pipe:
        numbers = extract_numbers_from_string(string)
        all_numbers.append(numbers)
    all_numbers = [row for row in all_numbers if row]
    # Fill variables
    if len(all_numbers) > 0:
        # Check if the first list of numbers has more than 4 elements
        if len(all_numbers[0]) > 4:
            code_etablissement = "".join(map(str, all_numbers[0][:3]))  # Take the first 3 numbers
            date = "{}{}{}".format(all_numbers[0][3], all_numbers[0][4], "".join(map(str, all_numbers[0][5:]))) 
            if len(all_numbers) > 1 and len(all_numbers[1]) > 2:
                codedoc = "{:03d}".format(int("".join(map(str, all_numbers[1]))))  # Take all numbers
                if len(all_numbers) > 2 and len(all_numbers[2]) > 0:
                    codemon = str(all_numbers[2][0])
            else:
                codemon = str(all_numbers[1][0])
        else:
            if len(all_numbers[0]) > 2:
                codedoc = "{:03d}".format(int("".join(map(str, all_numbers[0]))))  # Take all numbers
                if len(all_numbers) > 1 and len(all_numbers[1]) > 0 and len(all_numbers[2]) > 0:
                    codemon = str(all_numbers[1][0])
            else:
                if len(all_numbers) > 1 and len(all_numbers[1]) > 0:
                    codemon = str(all_numbers[2][0])
    else:  
        return "", "", "", "", "", True
    # Return filled variables
    return code_etablissement, date, codedoc, codemon, compteur, end_of_file


#affichage de tableau
def display_data_with_style(data):
    # Define CSS styles
    styles = [
        {'selector': 'thead th', 'props': [('background-color', 'lightblue'), ('color', 'black')]},
        {'selector': 'tbody td', 'props': [('background-color', 'lightgrey'), ('color', 'black'), ('font-weight', 'bold')]},
        {'selector': 'tbody tr:nth-child(even)', 'props': [('background-color', 'white')]},
        {'selector': 'tbody tr:nth-child(odd)', 'props': [('background-color', 'lightyellow')]},
    ]
    
    # Construct the CSS string
    css_string = ''.join([f"{s['selector']} {{{' '.join([f'{p[0]}: {p[1]};' for p in s['props']])}}}" for s in styles])
    
    # Combine the HTML table and CSS styles
    html_table_with_style = f"<style>{css_string}</style>{data.to_html(classes='table table-striped', index=False, table_id='styled_table', justify='center')}"
    
    # Display the HTML table with styles
    #display(HTML(html_table_with_style))


def remove_empty_rows_and_title(data):
    # Rename the first cell of the first row to "code"
    data.iloc[0, 1] = "code"
    data.iloc[0, 0] = "num ligne"
    
    # Drop rows where the second cell is NaN, except for the first row
    data = data.dropna(subset=[data.columns[1]], thresh=1)
    
    # Drop columns where the first cell is NaN
    data = data.dropna(axis=1, subset=[0])
    
    #remplacera à la fois les valeurs NaN par des zéros et toutes les occurrences de "////.." par des zéros 
    data = data.fillna(0).replace(re.compile(r'/+'), 0)
    
    return data


#pour lire tous les document d'un feuil
def LireTousDocuments1feuil(dataExcel, file_path, sheet_name,collection):
    cmpfin = 0
    
    while True:  # Boucle infinie
        start_row = cmpfin
        data = dataExcel
        code_etablissment, date, codedoc, codemon, compteur, end_of_file = recuperer_entete_document(data, start_row,sheet_name)
        
        if end_of_file:
            #print("Fin de fichier")
            break
            
        # print("code etablissment:", code_etablissment)
        # print("date:", date)
        # print("codedoc:", codedoc)
        # print("codemon:", codemon)
        # print("etat:", sheet_name)
        #print("filio:", filio)
        #print("ligne:", compteur)

        # **************************chercher le debut et la fin de document********************************************
        cmpfin = compteur
        cmpdebut = compteur + 1
        
        # Jusqu'à la fin ou le début du 2eme document (début du 2eme document avec |)
        for index, row in data.iloc[cmpfin:].iterrows():
            # Check if the first cell value contains "Code ligne"
            if '|' in str(row.iloc[0]) or '(' in str(row.iloc[0]):
            #if '|' in str(row.iloc[0]) or '(' in str(row.iloc[0]) or len(str(row.iloc[0])) > 10:
            #if '|' in str(row.iloc[0]) or '(' in str(row.iloc[0]) or 'T' in str(row.iloc[0]) :
                break  # Exit the loop if condition met
            #cas particulier
            if sheet_name !="Etat 014" and sheet_name!="Etat 012" and 'T' in str(row.iloc[0]):
                break
            cmpfin += 1
        
        # fin de document
        #print(cmpfin)

        # commence avec indice de ligne different de null après code ligne
        for index, row in data.iloc[cmpdebut:].iterrows():
            # Check if the first cell value contains "Code ligne"
            if pd.notna(row.iloc[0]):
                break  # Exit the loop if condition met
            cmpdebut += 1
        cmpdebut -= 1

        # ****************************Lire le document********************************************************************
        # Read Excel file starting from row cmpdebut and stopping at row cmpfin
        data = pd.read_excel(file_path, sheet_name=sheet_name, header=None, skiprows=cmpdebut, nrows=cmpfin - cmpdebut)

        # Ajouter une colonne avec le numéro de ligne
        data.insert(0, 'Numero_de_ligne', range(cmpdebut + 1, cmpfin + 1))

        # *****************Supprimer les lignes et les colonnes non utilisées et ajouter le numero de ligne dans excel****
        # Remove empty rows and title
        data1 = remove_empty_rows_and_title(data)

        header_list = data1.iloc[0].values.tolist()
       
        # Convertir les valeurs numériques en chaînes de caractères sans le point décimal
        header_list = [str(int(header)) if isinstance(header, float) else str(header) for header in header_list]

        #print(header_list)

        # Remplacer les noms de colonnes par les valeurs de la première ligne
        data1.columns = header_list

        # Supprimer la première ligne
        data1 = data1.drop(data1.index[0])

        # Print the data
        #display_data_with_style(data1)      

        # Créer une liste pour stocker les données formatées
        data_list = []

        # Itérer sur chaque ligne du DataFrame
        for index, row in data1.iterrows():
            # Convertir la ligne actuelle en un dictionnaire JSON
            data_json = row.to_dict()

            # Ajouter le dictionnaire JSON à la liste
            data_list.append(data_json)

        # Créer le document final
        document = {
            "code_etablissment": code_etablissment,
            "date": date,
            "codedoc": codedoc,
            "codemon": codemon,
            "etat":sheet_name,
            "data": data_list
        }

        # Insérer le document dans MongoDB
        collection.insert_one(document)

        #print("Données insérées avec succès dans MongoDB.")
        
        #break

#//////////////////////////////////////////////FIN TRAITEMENT///////////////////////////////////////////////////////
#///////////////////////////////////////////////////////////////////////////////////////////////////////////////

# Define the process_sheet function outside of appelfonction
def process_sheet(file_path, sheet_name, collection):
    dataExcel = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    LireTousDocuments1feuil(dataExcel, file_path, sheet_name, collection)
    print(f"Finished processing {sheet_name}")

def appelfonction(file_path,collecname):
    start_time = time.time()  # Record the start time

    # Extract file name from the complete path
    file_name = os.path.splitext(os.path.basename(file_path))[0]

    # Connect to MongoDB
    client = MongoClient('localhost', 27017)  # Replace 'localhost' and 27017 with your MongoDB address and port
    db = client['db_etat_comptable']  # Replace 'db_etat_comptable' with the name of your database

    # Get MongoDB collection
    collection = db[collecname]

    excel_file = pd.ExcelFile(file_path)

    # Get all sheet names
    sheet_names = excel_file.sheet_names

    # Remove unwanted sheets
    for sheet in ["Etat 026", "Etat 028"]:
        if sheet in sheet_names:
            sheet_names.remove(sheet)

    # Use ThreadPoolExecutor to process sheets in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit tasks to the executor for each sheet
        futures = [executor.submit(process_sheet, file_path, sheet_name, collection) for sheet_name in sheet_names[:-2]]
        
        # Ensure all futures are completed
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # Get the result to catch any exceptions raised
            except Exception as exc:
                print(f"{sheet_name} generated an exception: {exc}")
        
    end_time = time.time()  # Record the end time
    execution_time = end_time - start_time  # Calculate the execution time
    print(f"Total execution time: {execution_time} seconds")

# #///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
##reponse avec page
def home(request):
    if 'username' not in request.session:
        return redirect('login')
    return render(request,'home.html',{'name':'Aya'})




#/////////////////////////////////////Spark ETL with NonSQL Database (MongoDB)/////////////////////////////////////
#//////////////////////////////////////////////////////////////////////////////////////////////////////////////////



def process_data_to_text(file_path,collecname):
    file_name = os.path.splitext(os.path.basename(file_path))[0]
    

    spark = SparkSession.builder.appName("ETAT COMPTABLE")\
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1')\
            .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")    
    mongodf = spark.read.format("mongo") \
        .option("uri", "mongodb://localhost:27017/") \
        .option("database", "db_etat_comptable") \
        .option("collection", collecname) \
        .load()

    mongodf.createOrReplaceTempView("tempMongo")
    sqlContext = SparkSession(spark)
    newdf = sqlContext.sql("SELECT code_etablissment,date,codedoc,codemon,etat,data FROM tempMongo")

    now = datetime.now()
    formatted_date_time = now.strftime("%S%M%H%d%m%Y")
    nomfichier = "DCC_012_001_" + formatted_date_time
    file_path_with_name = os.path.join(os.path.dirname(file_path), nomfichier)

    with open(file_path_with_name, "w") as file:
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

        file.write(formatted_element.get("code_etablissment") + formatted_element.get("date") + str(newdf.count()).zfill(3))
        file.write("\n")

        codedocrep = ""

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
                file.write(formatted_element.get("codedoc") + str(len(data)).zfill(3))
                file.write("\n")
                codedocrep = formatted_element.get("codedoc")

            colonnes_a_ne_pas_afficher = ["code", "num ligne"]

            for row_data in data:
                for column, value in row_data.asDict().items():
                    if value is not None and column not in colonnes_a_ne_pas_afficher:
                        formatted_value = str(value).replace(".", "").zfill(13)
                        try:
                            if float(value) >= 0:
                                file.write(formatted_element.get("codemon") + row_data["code"] + column.zfill(2) + "C" + formatted_value)
                            else:
                                file.write(formatted_element.get("codemon") + row_data["code"] + column + "D" + formatted_value)
                            file.write("\n")
                        except ValueError:
                            file.write(formatted_element.get("codemon") + row_data["code"] + column + "D" + str(0).replace(".", "").zfill(13))
                            file.write("\n")
    return file_path_with_name,nomfichier



# Fonction pour vérifier si la collection existe
def collection_exists(database_name, collection_name):
    client = MongoClient('localhost', 27017)
    if collection_name in client[database_name].list_collection_names():
        return True
    else:
        return False

@csrf_exempt
def add(request):
    if 'username' not in request.session:
        return redirect('login')
    if request.method == 'POST' and request.FILES.get('file'):
        uploaded_file = request.FILES['file']
        mois = request.POST.get('mois')
        annee = request.POST.get('annee')

        # Générer un identifiant unique pour le fichier téléchargé
        file_id = uuid.uuid4().hex

        # Enregistrer le fichier dans un emplacement temporaire
        file_path = os.path.normpath(os.path.join('C:/Users/DELL/projects/aya/temporaire', file_id + '_' + uploaded_file.name))

        # Écrire le contenu du fichier dans l'emplacement temporaire
        with open(file_path, 'wb+') as destination:
            for chunk in uploaded_file.chunks():
                destination.write(chunk)
        
        database_name = "db_etat_comptable"
        spark = SparkSession.builder.appName("ETAT COMPTABLE")\
                .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1')\
                .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        
        # Vérifier si la collection existe déjà
        if collection_exists(database_name, mois + "/" + annee):
            # Afficher une boîte de dialogue modale pour confirmer l'action
            return render(request, 'confirm_dialog.html', {'file_path': file_path, 'collection_name': mois + "/" + annee})
        else:
            # Appeler la fonction directement
            appelfonction(file_path, mois + "/" + annee)
            nomfichier1,nomfichier = process_data_to_text(file_path, mois + "/" + annee)
            
            preview = "Traitement terminé."
            return render(request, 'upload_form.html', {'preview': nomfichier1,'nom':nomfichier})

    else:
        preview = "Traitement non terminé."
        return render(request, 'upload_form.html', {'preview': preview})


# Vue pour le formulaire de confirmation
def confirm_dialog(request):
    if request.method == 'POST':
        file_path = request.POST['file_path']
        collection_name = request.POST['collection_name']
        # Appeler la fonction pour modifier la collection existante
        appelfonction(file_path, collection_name)
        nomfichier1,nomfichier = process_data_to_text(file_path, collection_name)
        preview = "Données mises à jour avec succès."
        return render(request, 'upload_form.html', {'preview': nomfichier1,'nom':nomfichier})
    else:
        return HttpResponse("Méthode non autorisée")


from urllib.parse import unquote

from django.views.decorators.csrf import csrf_exempt
from urllib.parse import unquote

def vider_collection(collection_name):
    # Connexion à MongoDB
    client = MongoClient('localhost', 27017)
    db = client['db_etat_comptable']
    
    # Vider le contenu de la collection
    result = db[collection_name].delete_many({})
    
@csrf_exempt
def modifier_collection(request):
    if 'username' not in request.session:
        return redirect('login')
    if request.method == 'POST':
        # Récupérer et décoder les paramètres POST
        file_path = unquote(request.POST.get('file_path'))
        collection_name = unquote(request.POST.get('collection_name'))
        
        # Vider le contenu de la collection
        vider_collection(collection_name)

        # Appeler la fonction pour modifier la collection existante
        appelfonction(file_path, collection_name)
        nomfichier1,nomfichier = process_data_to_text(file_path, collection_name)
        preview = "Traitement terminé."
        return render(request, 'upload_form.html', {'preview': nomfichier1,'nom':nomfichier})
    else:
        return HttpResponse("Méthode non autorisée")

@csrf_exempt
def annuler_modification(request):
    if 'username' not in request.session:
        return redirect('login')
    if request.method == 'POST':
        # Récupérer et décoder les paramètres POST
        file_path = unquote(request.POST.get('file_path'))
        collection_name = unquote(request.POST.get('collection_name'))
        nomfichier1,nomfichier = process_data_to_text(file_path, collection_name)
        preview = "Traitement terminé."
        return render(request, 'upload_form.html', {'preview': nomfichier1,'nom':nomfichier})
    else:
        # return HttpResponse("Méthode annulée")
        return render(request, 'Remise.html')



def download_file(request, filename,nomf):
    if 'username' not in request.session:
        return redirect('login')
    file_path = os.path.join(settings.BASE_DIR, 'temporaire', filename)
    
    if os.path.exists(file_path):
        # Renommer le fichier
        new_filename = nomf
        new_file_path = os.path.join(os.path.dirname(file_path), new_filename)
        os.rename(file_path, new_file_path)

        # Ouvrir le fichier renommé en mode binaire pour lecture
        response = FileResponse(open(new_file_path, 'rb'))
        
        # Définir l'en-tête Content-Disposition pour déclencher le téléchargement
        response['Content-Disposition'] = f'attachment; filename="{new_filename}"'
        
        return response
    else:
        return HttpResponse('File not found: ' + filename, status=404)


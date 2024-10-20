from datetime import datetime
import os
import re
import pandas as pd
import re
import os
import json
from pymongo import MongoClient
from pyspark.sql import SparkSession
import uuid
from django.shortcuts import render
from django.http import FileResponse, HttpResponse
from django.conf import settings
from urllib.parse import quote
from django.shortcuts import render, redirect

#erreur 002    
# Fonction pour vérifier la dernière ligne et écrire les erreurs dans un fichier
def verifier_derniere_ligne(fichier, fichier_sortie_obj):
    try:
        # Ouvrir le fichier en mode lecture
        with open(fichier, 'r') as f:
            # Lire toutes les lignes du fichier
            lignes = f.readlines()
            
            # Vérifier s'il y a au moins une ligne dans le fichier
            if not lignes:
                fichier_sortie_obj.write("Le fichier est vide.\n")
                return
            
            # Obtenir la dernière ligne en supprimant les espaces et les caractères de nouvelle ligne
            derniere_ligne = lignes[-1].strip()
            
            # Décomposer la ligne selon les spécifications
            try:
                code_monnaie = derniere_ligne[0:1]
                ligne_document = derniere_ligne[1:5]
                rang_colonne = derniere_ligne[5:7]
                sens = derniere_ligne[7:8]
                montant = derniere_ligne[8:21]
                nbr_lignes = compter_lignes_sans_entete(fichier)
                
                # Vérifier chaque champ selon les spécifications
                if code_monnaie not in ['1', '2', '3']:
                    fichier_sortie_obj.write(f"DEC002{str(nbr_lignes).zfill(4)} Fin de Fichier incorrect.\n")
                    return
                
                if not ligne_document.isalnum() or len(ligne_document) != 4:
                    fichier_sortie_obj.write(f"DEC002{str(nbr_lignes).zfill(4)} Fin de Fichier incorrect.\n")
                    return
    
                if not rang_colonne.isdigit() or len(rang_colonne) != 2:
                    fichier_sortie_obj.write(f"DEC002{str(nbr_lignes).zfill(4)} Fin de Fichier incorrect.\n")
                    return
                
                if sens not in ['D', 'C']:
                    fichier_sortie_obj.write(f"DEC002{str(nbr_lignes).zfill(4)} Fin de Fichier incorrect.\n")
                    return
                
                if not montant.isdigit() or len(montant) != 13:
                    fichier_sortie_obj.write(f"DEC002{str(nbr_lignes).zfill(4)} Fin de Fichier incorrect.\n")
                    return
                
                # Si tous les contrôles passent, la ligne est valide
                fichier_sortie_obj.write(f"DEC002{str(nbr_lignes).zfill(4)} Fin de Fichier incorrect.\n")
                return
            
            except IndexError:
                fichier_sortie_obj.write(f"DEC002{str(nbr_lignes).zfill(4)} Fin de Fichier incorrect.\n")
                return
        
    except FileNotFoundError:
        fichier_sortie_obj.write("Le fichier spécifié n'existe pas.\n")
        return
    


#004 005 003 114  plus 108
def verifier_entete_remise_et_documents(fichier, fichier_sortie_obj):
    try:
        # Ouvrir le fichier en mode lecture
        with open(fichier, 'r') as f:
            try:
                test = False
                # Ouvrir le fichier en mode lecture
                with open(fichier, 'r') as f:
                    lignes = f.readlines()

                if not lignes:
#                     fichier_sortie_obj.write("Le fichier est vide.")
                    return

                # Extraire l'en-tête de remise de la première ligne
                entete_remise = lignes[0].strip()

                # Vérifier l'en-tête de remise
                if len(entete_remise) != 12 or not entete_remise.isdigit():
                    fichier_sortie_obj.write(f"DEC004001 Entête remise non conforme. Référence: {entete_remise[:3]}. Numéro de ligne : 1\n")

                code_etab = entete_remise[0:2]
                if(code_etab!= "012"):
                    fichier_sortie_obj.write(f"DEC108001 Code de l’établissement invalide. Référence: {code_etab}. Numéro de ligne : 1\n")



                # Extraire le nombre de documents remis de l'en-tête de remise
                nombre_documents_remis_str = entete_remise[9:12]
                if not nombre_documents_remis_str.isdigit():
                    test = True
                else:
                    nombre_documents_remis = int(nombre_documents_remis_str)
                    # Vérifier la date d'arrêt dans l'entête de remise
                    mois = int(entete_remise[3:5])
                    annee = int(entete_remise[6:])
                    if not (1 <= mois <= 12 and 1900 <= annee <= 9999):
                        fichier_sortie_obj.write("DEC114001 Date d'arrêt invalide dans l'entête de remise.\n")

                # Initialiser un dictionnaire pour stocker les documents et leurs lignes de détail
                documents = {}
                documents_comptes = 0
                i = 1  # Numéro de ligne

                # Parcourir les lignes du fichier à partir de la deuxième ligne
                while i < len(lignes):
                    ligne = lignes[i].strip()

                    # Vérifier si la ligne correspond à un en-tête de document
                    if len(ligne) == 6 and ligne.isdigit():
                        code_document = ligne[:3]
                        nombre_lignes_detail = int(ligne[3:])

                        # Vérifier si le document existe déjà
                        if code_document in documents:
        #                     print(f"Document transmis en double. Référence: {code_document}.")
                            fichier_sortie_obj.write(f"DEC003{str(i+1).zfill(4)} Document transmis en double. Référence: {code_document}. Numéro de ligne : {i+1}\n")
                        else:
                            documents[code_document] = nombre_lignes_detail
                            documents_comptes += 1

                        # Passer aux prochaines lignes de détail
                        i += 1 + nombre_lignes_detail
                    else:
                        # Si la ligne ne correspond pas à un en-tête de document valide, passer à la suivante
                        i += 1

                # Si le nombre de documents n'est pas un nombre entier, arrêter la fonction
                if test:
                    return

                # Comparer le nombre de documents attendus avec le nombre de documents trouvés
                if nombre_documents_remis != documents_comptes:
                    fichier_sortie_obj.write(f"DEC005001 Nombre de documents incohérents. Référence: {entete_remise[:3]}. Numéro de ligne : 1\n")

        #         # Afficher tous les documents trouvés
        #         for doc, nb_lignes in documents.items():
        #             print(f"Document: {doc}, Nombre de lignes de détail: {nb_lignes}")

            except FileNotFoundError:
                fichier_sortie_obj.write("Le fichier spécifié n'existe pas.\n")
                return
    except FileNotFoundError:
#         fichier_sortie_obj.write("Le fichier spécifié n'existe pas.\n")
        return


#en ajoutant rang dans mod d doublons pour codeligne
#erreur 006 007 010 015 011 103 101
def verifier_entetes_et_contenu_documents(fichier, fichier_sortie_obj):
    try:
        # Ouvrir le fichier en mode lecture
        with open(fichier, 'r') as f:
            
            try:
                # Ouvrir le fichier en mode lecture
                with open(fichier, 'r') as f:
                    lignes = f.readlines()

                if not lignes:
#                     print("Le fichier est vide.")
                    return

                contenu_documents = {}

                # Parcourir les lignes du fichier à partir de la deuxième ligne
                i = 1
                while i < len(lignes):
                    ligne = lignes[i].strip()

                    # Vérifier si la ligne correspond à un en-tête de document
                    if len(ligne) == 6:
                        code_document = ligne[:3]
                        nombre_lignes_detail = ligne[3:]

                        # Vérifier les spécifications de l'en-tête de document
                        if not code_document.isalnum() or len(code_document) != 3 or not nombre_lignes_detail.isdigit() or len(nombre_lignes_detail) != 3:
                            fichier_sortie_obj.write(f"DEC006{str(ligne[:5]).zfill(4)} Entête document non conforme. Référence: {ligne[:5]}. Numéro de ligne : {i+1}\n")

                        # Vérifier si le nombre de lignes de détail est valide
                        if nombre_lignes_detail.isdigit():
                            nombre_lignes_detail_int = int(nombre_lignes_detail)
                            if nombre_lignes_detail_int <= 0:#here
                                fichier_sortie_obj.write(f"DEC007{str(i+1).zfill(4)} Ligne document non conforme {ligne[:5]}. Numéro de ligne : {i+1}\n")

                        lignes_document = set()
                        contenu_documents[code_document] = lignes_document

                        # Parcourir les lignes de détail de ce document
                        j = i + 1  # Initialiser j

                        # Boucle jusqu'à ce qu'une nouvelle entête soit rencontrée
                        while j < len(lignes) and len(lignes[j].strip()) != 6:
                            ligne_detail = lignes[j].strip()

                            # Vérifier la longueur de la ligne de détail
                            if len(ligne_detail) != 21:#here
                                fichier_sortie_obj.write(f"DEC007{str(j+1).zfill(4)} Ligne document non conforme. Référence: {ligne[:5]}. Numéro de ligne : {j+1}\n")
                                j += 1
                                continue

                            # Vérifier chaque champ
                            code_monnaie = ligne_detail[0:1]
                            ligne_document = ligne_detail[1:5] if ligne_detail[1:5] != "    " else "0000"
                            rang_colonne = ligne_detail[5:7]
                            sens = ligne_detail[7:8]
                            montant = ligne_detail[8:21]

                            if code_monnaie not in {'1', '2', '3'}:
                                fichier_sortie_obj.write(f"DEC010{str(j+1).zfill(4)} Code monnaie invalide. Référence: {ligne[:5]}. Numéro de ligne : {j+1}\n")
                            if not ligne_document.isalnum():
                                fichier_sortie_obj.write(f"DEC007{str(j+1).zfill(4)} Ligne document non conforme. Référence: {ligne[:5]}. Numéro de ligne : {j+1}\n")
                            if not rang_colonne.isdigit():
                                fichier_sortie_obj.write(f"DEC103{str(j+1).zfill(4)} Rang de la colonne invalide Référence: {ligne[:5]}. Numéro de ligne : {j+1}\n")
                            if sens not in {'D', 'C'}:
                                fichier_sortie_obj.write(f"DEC015{str(j+1).zfill(4)} Sens non conforme. Référence: {ligne[:5]}. Numéro de ligne : {j+1}\n")
                            if not montant.isdigit():
                                fichier_sortie_obj.write(f"DEC007{str(j+1).zfill(4)} Montant non conforme. Référence: {ligne[:5]}. Numéro de ligne : {j+1}\n")

                            # Vérifier les doublons de lignes de document
                            if (code_monnaie, ligne_document, rang_colonne) in lignes_document:
                                fichier_sortie_obj.write(f"DEC011{str(j+1).zfill(4)} Ligne de document en double. Référence: {ligne[:5]}. Numéro de ligne : {j+1}\n")
                            else:
                                lignes_document.add((code_monnaie, ligne_document, rang_colonne))

                            j += 1

                        # Mettre à jour l'index pour sauter les lignes de détail traitées
                        i = j
                    else:
                        i += 1

            except FileNotFoundError:
#                 print("Le fichier spécifié n'existe pas.")
                return
    except FileNotFoundError:
#         fichier_sortie_obj.write("Le fichier spécifié n'existe pas.\n")
        return


#erreur 012 013
def extraire_et_verifier_nom_fichier(chemin_fichier, fichier_sortie_obj):
    try:
        # Ouvrir le fichier en mode lecture
        with open(chemin_fichier, 'r') as f:
            # Extraire le nom de fichier avec extension
            nom_fichier_complet = os.path.basename(chemin_fichier)

            # Enlever l'extension si elle existe
            nom_fichier_sans_ext, extension = os.path.splitext(nom_fichier_complet)

            # Vérifier si le fichier a une extension
            if extension:
                fichier_sortie_obj.write("DEC01200000 Type de fichier invalide.\n")

            # Définir le modèle regex pour valider le nom du fichier
            regex = r'^DCC_\d{3}_\d{3}_\d{14}$'

            # Vérifier si le nom du fichier respecte le format
            if re.match(regex, nom_fichier_sans_ext):
                # Extraire les parties du nom du fichier
                try:
                    _, code_emetteur, code_recepteur, date_heure = nom_fichier_sans_ext.split('_')

                    # Valider les parties
                    if not (code_emetteur.isdigit() and len(code_emetteur) == 3):
                        fichier_sortie_obj.write("DEC0130000 Le nom du fichier de remise invalide.\n")
                        return
                    if not (code_recepteur.isdigit() and len(code_recepteur) == 3):
                        fichier_sortie_obj.write("DEC0130000 Le nom du fichier de remise invalide.\n")
                        return

                    # Valider le format de la date et de l'heure
                    try:
                        datetime.strptime(date_heure, "%M%S%H%d%m%Y")
                    except ValueError:
                        fichier_sortie_obj.write("DEC0130000 Le nom du fichier de remise invalide.\n")
                        return

                    # Si toutes les validations passent
                    return

                except Exception as e:
                    fichier_sortie_obj.write("DEC0130000 Le nom du fichier de remise invalide.\n")
                    return
            else:
                fichier_sortie_obj.write("DEC0130000 Le nom du fichier de remise invalide.\n")
                return
            
    except FileNotFoundError:
        fichier_sortie_obj.write("Le fichier spécifié n'existe pas.\n")
        return



#erreur 008
def compter_lignes_distinctes(lignes):
    lignes_uniques = set()
    
    for ligne in lignes:
        code_monnaie = ligne[0]
        code_ligne = ligne[1:5]
        sens = ligne[7]
        
        # Ajouter une entrée unique basée sur code monnaie, code ligne, et sens
        lignes_uniques.add((code_monnaie, code_ligne, sens))
    
    return len(lignes_uniques)

def verifier_et_compter_lignes(fichier, fichier_sortie_obj):
    try:
        # Ouvrir le fichier en mode lecture
        with open(fichier, 'r') as f:

            try:
                # Ouvrir le fichier en mode lecture
                with open(fichier, 'r') as f:
                    lignes = f.readlines()

                if not lignes:
#                     fichier_sortie_obj.write("Le fichier est vide.")
                    return

                contenu_documents = {}

                # Parcourir les lignes du fichier
                i = 0
                while i < len(lignes):
                    ligne = lignes[i].strip()

                    # Vérifier si la ligne correspond à un en-tête de document
                    if len(ligne) == 6:
                        code_document = ligne[:3]
                        nombre_lignes_detail = ligne[3:]

                        # Afficher l'entête du document
                        #print(f"Traitement de l'entête du document: {ligne}")

                        lignes_document = []

                        # Parcourir les lignes de détail de ce document
                        j = i + 1  # Initialiser j

                        # Boucle jusqu'à ce qu'une nouvelle entête soit rencontrée
                        while j < len(lignes) and len(lignes[j].strip()) != 6:
                            ligne_detail = lignes[j].strip()

                            # Ajouter la ligne de détail à la liste
                            lignes_document.append(ligne_detail)
                            j += 1

                        # Ajouter les lignes de détail au contenu du document
                        contenu_documents[code_document] = lignes_document

                        # Calculer et afficher le nombre de lignes distinctes pour ce document
                        nombre_lignes_distinctes = compter_lignes_distinctes(lignes_document)
                        #print(f"Document {code_document}: Nombre de lignes distinctes = {nombre_lignes_distinctes}")

                        # Comparer les trois derniers caractères de l'entête avec le nombre de lignes distinctes
                        if nombre_lignes_distinctes != int(nombre_lignes_detail):
                            fichier_sortie_obj.write(f"DEC008{str(i+1).zfill(4)} Nombre d'enregistrements non cohérents. Référence: {ligne}. Numéro de ligne : {i+1}\n")

                        # Mettre à jour l'index pour sauter les lignes de détail traitées
                        i = j
                    else:
                        i += 1

            except FileNotFoundError:
#                 print("Le fichier spécifié n'existe pas.")
                return
            
    except FileNotFoundError:
#         fichier_sortie_obj.write("Le fichier spécifié n'existe pas.\n")
        return


#erreur 102
def load_existing_codes():
    # Connexion à MongoDB
    client = MongoClient('localhost', 27017)  
    db = client['db_etat_comptable']
    collection = db['categories']
    
    # Charger les codes ligne existants
    existing_codes = set()
    for doc in collection.find({}, {"code": 1}):
        existing_codes.add(doc['code'])
    
    return existing_codes

def verifier_code_ligne(fichier, fichier_sortie_obj):
    
    try:
        # Ouvrir le fichier en mode lecture
        with open(fichier, 'r') as f:

            existing_codes = load_existing_codes()

            try:
                with open(fichier, 'r') as f:
                    lignes = f.readlines()

                if not lignes:
                    return

                # Parcourir les lignes du fichier
                for i, ligne in enumerate(lignes):
                    ligne = ligne.strip()

                    # Vérifier si la ligne correspond à un en-tête de document
                    if len(ligne) == 6:
                        # Parcourir les lignes de détail de ce document
                        j = i + 1
                        while j < len(lignes) and len(lignes[j].strip()) != 6:
                            ligne_detail = lignes[j].strip()

                            if len(ligne_detail) != 21:
                                j += 1
                                continue

                            ligne_document = ligne_detail[1:5] if ligne_detail[1:5] != "    " else "0000"

                            # Vérifier si le code ligne existe
                            if ligne_document not in existing_codes:
                                fichier_sortie_obj.write(f"DEC102{str(j+1).zfill(4)} Code ligne erroné. Référence: {ligne_document}. Numéro de ligne : {j+1}\n")
        #                     else:
        #                         print(f"Code ligne existe")

                            j += 1

                        # Mettre à jour l'index pour sauter les lignes de détail traitées
                        i = j
                    else:
                        i += 1

            except FileNotFoundError:
#                 print("Le fichier spécifié n'existe pas.")
                  return

    except FileNotFoundError:
#         fichier_sortie_obj.write("Le fichier spécifié n'existe pas.\n")
        return



def compter_lignes_sans_entete(chemin_fichier):
    with open(chemin_fichier, 'r', encoding='utf-8') as fichier:
        lignes = fichier.readlines()
        # Soustraire une ligne pour l'en-tête
        return len(lignes) - 1

def fiche_compte_rendu (chemin_fichier):  
    now = datetime.now()
    formatted_date_time = now.strftime("%S%M%H%d%m%Y")
    nomcompterendu = "DCC_012_001_" + formatted_date_time
    #entete de fichier
    nom_fichier = os.path.basename(chemin_fichier)
    # Extraire les 14 derniers caractères du nom de fichier (sans l'extension)
    nom_de_base, extension = os.path.splitext(nom_fichier)
    derniers_14_caracteres = nom_de_base[-14:]
    # Obtenir l'heure actuelle et formater la date et l'heure
    now = datetime.now()
    formatted_date_time = now.strftime("%S%M%H%d%m%Y")
    # Créer l'en-tête
    entete = "ECR001012" + formatted_date_time + "02"+derniers_14_caracteres


    with open(nomcompterendu, 'w', encoding='utf-8') as fichier:
        # Écrire l'en-tête dans le fichier
        fichier.write(entete + '\n')

        # Appel de la fonction pour vérifier la dernière ligne
        verifier_derniere_ligne(chemin_fichier, fichier)

        # Erreur 004, 005, 003, 114
        verifier_entete_remise_et_documents(chemin_fichier, fichier)

        # Erreur 006, 007, 010, 015, 011, 103, 101
        verifier_entetes_et_contenu_documents(chemin_fichier, fichier)

        # Erreur 012, 013
        extraire_et_verifier_nom_fichier(chemin_fichier, fichier)

        # Erreur 008
        verifier_et_compter_lignes(chemin_fichier, fichier)

        # Erreur 102
        verifier_code_ligne(chemin_fichier, fichier)

    # Calculer le nombre de lignes moins l'entête
    nbr_lignes = compter_lignes_sans_entete(nomcompterendu)

    # Ajouter la fin de fichier avec le nombre de lignes moins l'entête
    with open(nomcompterendu, 'a', encoding='utf-8') as fichier:
        fichier.write('FCR' + str(nbr_lignes).zfill(4))

    print(f"Le fichier {nomcompterendu} a été créé avec l'en-tête : {entete}")
    
    return nomcompterendu



# def compte_rendu (request):
#     if request.method == 'POST' and request.FILES.get('file'):
#         uploaded_file = request.FILES['file']
#         # Générer un identifiant unique pour le fichier téléchargé
#         file_id = uuid.uuid4().hex

#         # Enregistrer le fichier dans un emplacement temporaire
#         file_path = os.path.normpath(os.path.join('C:/Users/DELL/projects/aya/temporaire', file_id + '_' + uploaded_file.name))
#         nomcompterendu=fiche_compte_rendu (file_path)
#         preview = "Traitement terminé."
#         chemin="C:/Users/DELL/projects/aya/"+ nomcompterendu
#         return render(request, 'upload_form.html', {'preview': nomcompterendu,'nom':chemin})
#     else:
#         preview = "Traitement non terminé."
#         return render(request, 'upload_form.html', {'preview': preview})
    

import uuid
import os
from django.shortcuts import render
from django.http import FileResponse, HttpResponse
from urllib.parse import quote

def compte_rendu(request):
    if 'username' not in request.session:
        return redirect('login')
    if request.method == 'POST' and request.FILES.get('file'):
        uploaded_file = request.FILES['file']
        # Générer un identifiant unique pour le fichier téléchargé
        file_id = uuid.uuid4().hex

        # Enregistrer le fichier dans un emplacement temporaire
        file_path = os.path.normpath(os.path.join('C:/Users/DELL/projects/aya/temporaire', file_id + '_' + uploaded_file.name))
        
        # Enregistrez le fichier sur le chemin spécifié
        with open(file_path, 'wb') as destination:
            for chunk in uploaded_file.chunks():
                destination.write(chunk)
        
        # Appel fictif à une fonction `fiche_compte_rendu` (vous devez la définir ou la remplacer par votre propre logique)
        nomcompterendu = fiche_compte_rendu(file_path)
        
        preview = "Traitement terminé."
        chemin = "C:/Users/DELL/projects/aya/" + nomcompterendu
        return render(request, 'upload_form2.html', {'preview': nomcompterendu, 'nom': chemin})
    else:
        preview = "Traitement non terminé."
        return render(request, 'upload_form2.html', {'preview': preview})




# def download_file1(request, filename, nomf):

#     if not filename or not nomf:
#         return redirect('ComptreRendu')
#     # Ajustez le chemin du fichier ici
#     file_path = os.path.join('C:/Users/DELL/projects/aya', filename)
    
#     if os.path.exists(file_path):
#         # Renommer le fichier
#         new_filename = filename
#         new_file_path = os.path.join(os.path.dirname(file_path), new_filename)
#         os.rename(file_path, new_file_path)

#         # Ouvrir le fichier renommé en mode binaire pour lecture
#         response = FileResponse(open(new_file_path, 'rb'))
        
#         # Définir l'en-tête Content-Disposition pour déclencher le téléchargement
#         response['Content-Disposition'] = f'attachment; filename="{quote(new_filename)}"'
        
#         return response
#     else:
#         return HttpResponse('File not found: ' + filename, status=404)



from django.shortcuts import render, redirect
from django.http import FileResponse, HttpResponse
import os
from urllib.parse import quote

def download_file1(request, filename, nomf):
    if not filename or not nomf:
        return redirect('ComptreRendu')
    # Ajustez le chemin du fichier ici
    file_path = os.path.join('C:/Users/DELL/projects/aya', filename)
    
    if os.path.exists(file_path):
        # Ouvrir le fichier en mode binaire pour lecture
        response = FileResponse(open(file_path, 'rb'))
        
        # Définir l'en-tête Content-Disposition pour déclencher le téléchargement
        response['Content-Disposition'] = f'attachment; filename="{quote(filename)}"'
        
        return response
    else:
        return HttpResponse(f'File not found: {filename}', status=404)

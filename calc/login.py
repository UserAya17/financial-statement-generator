from pymongo import MongoClient
from django.shortcuts import render, redirect
from django.contrib import messages
from django.contrib.auth.hashers import make_password, check_password

client = MongoClient('mongodb://localhost:27017/')
db = client['db_etat_comptable']
users_collection = db['users']

def register_view(request):
    if request.method == 'POST':
        username = request.POST['username']
        password = make_password(request.POST['password'])

        # Check if the user already exists
        if users_collection.find_one({'username': username}):
            messages.error(request, 'Username already exists.')
            return redirect('register')

        users_collection.insert_one({'username': username, 'password': password})
        messages.success(request, 'Registration successful. Please log in.')
        return redirect('login')
    return render(request, 'register.html')

def login_view(request):
    if request.method == 'POST':
        username = request.POST['username']
        password = request.POST['password']

        user = users_collection.find_one({'username': username})
        if user and check_password(password, user['password']):
            request.session['username'] = username
            request.session['role']= user.get('role', 'user')
            messages.success(request, 'Login successful.')
            return redirect('home')
        else:
            messages.error(request, 'Invalid username or password.')
            return redirect('login')
    return render(request, 'login.html')



def logout_view(request):
    request.session.flush()
    messages.success(request, 'Logged out successfully.')
    return redirect('login')

def home_view(request):
    if 'username' not in request.session:
        return redirect('login')
    return render(request, 'home1.html', {'username': request.session['username']})


from django.shortcuts import render

def gestion_compte_view(request):
    if request.session.get('role') != 'admin':
        return redirect('home')
    return render(request, 'gestion_compte.html')






#//////////////////////////////////////////////////////////////////////////
from django.shortcuts import render, redirect
from pymongo import MongoClient
from bson.objectid import ObjectId

# Connect to MongoDB
client = MongoClient('localhost', 27017)
db = client['db_etat_comptable']
users_collection = db['users']

from django.shortcuts import render, redirect
from pymongo import MongoClient
from bson.objectid import ObjectId

def users_list_view(request):
    users = list(users_collection.find())

    # Ajouter un attribut id pour chaque utilisateur
    for user in users:
        user['id'] = str(user['_id'])

    return render(request, 'users_list.html', {'users': users})

def delete_user_view(request, user_id):
    if request.method == 'POST':
        users_collection.delete_one({'_id': ObjectId(user_id)})
        return redirect('users_list')

    return redirect('users_list')


# def add_edit_user_view(request, user_id=None):
#     if request.method == "POST":
#         username = request.POST['username']
#         password = make_password(request.POST['password'])
#         role = request.POST['role']
#         if user_id:  # Update existing user
#             users_collection.update_one({"_id": ObjectId(user_id)}, {"$set": {"username": username, "password": password, "role": role}})
#         else:  # Add new user
#             users_collection.insert_one({"username": username, "password": password, "role": role})
#         return redirect('users_list')
    
#     user = users_collection.find_one({"_id": ObjectId(user_id)}) if user_id else None
#     return render(request, 'add_edit_user.html', {'user': user})


from django.shortcuts import render, redirect
from django.contrib.auth.hashers import make_password
from bson import ObjectId

def add_edit_user_view(request, user_id=None):
    if request.method == "POST":
        username = request.POST['username']
        password = make_password(request.POST['password'])
        role = request.POST['role']
        
        # Vérification de l'unicité du nom d'utilisateur
        if user_id:  # Mise à jour d'un utilisateur existant
            user = users_collection.find_one({"_id": ObjectId(user_id)})
            if user['username'] != username:
                existing_user = users_collection.find_one({"username": username})
                if existing_user:
                    error_message = "Le nom d'utilisateur existe déjà."
                    return render(request, 'add_edit_user.html', {'user': user, 'error_message': error_message, 'is_edit': True})
            users_collection.update_one({"_id": ObjectId(user_id)}, {"$set": {"username": username, "password": password, "role": role}})
        else:  # Ajout d'un nouvel utilisateur
            existing_user = users_collection.find_one({"username": username})
            if existing_user:
                error_message = "Le nom d'utilisateur existe déjà."
                return render(request, 'add_edit_user.html', {'error_message': error_message, 'is_edit': False, 'user': {'username': username}})
            users_collection.insert_one({"username": username, "password": password, "role": role})
        return redirect('users_list')
    
    user = users_collection.find_one({"_id": ObjectId(user_id)}) if user_id else None
    return render(request, 'add_edit_user.html', {'user': user, 'is_edit': bool(user_id)})

# # mapping for home page
# from django.urls import path

# from . import views
# from django.conf.urls import url

# urlpatterns = [
#     path('',views.home,name='home'),
#     path('add',views.add,name='home'),
#     url(r'^download/(?P<filename>[^/]+)$', views.download_file, name='download_file'),
    
# ]


from django.urls import path, re_path

from . import views,erreur,regle,login



urlpatterns = [
    path('', login.login_view, name='login'),
    path('add', views.add, name='add'),
    path('compterendu', erreur.compte_rendu, name='compterendu'),
    path('remise/', views.remise_view, name='remise'), 
    path('ComptreRendu/', views.ComptreRendu_view, name='ComptreRendu'),
    path('Categories/', views.load_existing_categories, name='Categories'),
    path('Etat/', views.load_existing_etat, name='Etat'),
    path('Verticale/', views.Verticale_view, name='Verticale'), 
    path('Horizontale/', views.Horizontale_view, name='Horizontale'),
    path('InterDocument/', views.InterDocument_view, name='InterDocument'),
    path('confirm_dialog', views.confirm_dialog, name='confirm_dialog'),
    path('modifier', views.modifier_collection, name='modifier'),
    path('annuler', views.annuler_modification, name='annuler'),
    re_path(r'^download/(?P<filename>[^/]+)/(?P<nomf>[^/]+)$', views.download_file, name='download_file'),
    re_path(r'^download/(?P<filename>[^/]+)/(?P<nomf>.+)/$', erreur.download_file1, name='download_file1'),
    path('postRegleVerticale/', regle.postRegleVerticale, name='postRegleVerticale'), 
    path('postRegleHorizontale/', regle.postRegleHorizontale, name='postRegleHorizontale'),
    path('postRegleInterDocument/', regle.postRegleInterDocument, name='postRegleInterDocument'),





    path('register/', login.register_view, name='register'),
    path('login/', login.login_view, name='login'),
    path('logout/', login.logout_view, name='logout'),
    path('home/', views.home_view, name='home'),
    # path('gestion_compte/', login.users_list_view, name='gestion_compte'),


    path('users/', login.users_list_view, name='users_list'),
    path('users/add/', login.add_edit_user_view, name='add_user'),
    path('users/edit/<str:user_id>/', login.add_edit_user_view, name='edit_user'),
    path('users/delete/<str:user_id>/', login.delete_user_view, name='delete_user')
    
]
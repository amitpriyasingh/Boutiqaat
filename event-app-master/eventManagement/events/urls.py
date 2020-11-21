from django.urls import path

from . import views

urlpatterns = [
    path('celebrity_mapping/', views.celebrity_mapping, name='celebrity_mapping'),
    path('preview/', views.preview, name='preview'),
    path('getproduct/', views.getproductid, name='getproductid'),
    path('success/', views.success, name='success'),
    path('updateSuccess/', views.updateSuccess, name='updateSuccess'),
    #url(r'^select2/', include('django_select2.urls')),
]

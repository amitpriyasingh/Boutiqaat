"""eventManagement URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import include, path
from django.conf.urls.static import static
from django.contrib import admin
from django.contrib.staticfiles.urls import staticfiles_urlpatterns
from eventManagement import settings
from events import views
#from django.views.decorators.cache import cache_page
#from . import views

urlpatterns = [
    path('', views.welcome),
    path('events/', include('events.urls')),
    path('admin/events/eventslabeldetails/add/',views.celebrity_mapping),
    path('admin/events/eventslabeldetails/goback/', views.goback),
    path('admin/events/eventslabeldetails/<int:id>/change/', views.update),
    path('admin/', admin.site.urls),
    #path('admin/', include('admin.urls')),
]
	
admin.site.site_header="Boutiqaat"
admin.site.site_title="Boutiqaat"
admin.site.index_title="Welcome To Boutiqaat Event Management App"

#admin.autodiscover()


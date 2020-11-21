from django.contrib import admin
from django.contrib.auth import get_user_model
from datetime import datetime, date, timedelta

from .models import *

admin.site.site_url='/admin/'

class CelebAdmin(admin.ModelAdmin):
    def formfield_for_foreignkey(self, db_field, request, **kwargs):
        if db_field.name == 'user':
            kwargs['queryset'] = get_user_model().objects.filter(username=request.user.username)
        return super(CelebAdmin, self).formfield_for_foreignkey(db_field, request, **kwargs)

    def get_readonly_fields(self, request, obj=None):
        if obj is not None:
            return self.readonly_fields + ('user',)
        return self.readonly_fields

    def add_view(self, request, form_url="", extra_context=None):
        data = request.GET.copy()
        data['user'] = request.user
        request.GET = data
        return super(CelebAdmin, self).add_view(request, form_url="", extra_context=extra_context)
    '''
    def formfield_for_manytomany(self, db_field, request, **kwargs):
        #if CelebAdmin.is_valid():
        data = CelebAdmin.form.cleaned_data
        celebrity = data['celebrity']
        print(celebrity)
        generic = data['generic']
        print(generic)
        if generic:
            if db_field.name == 'productid':
                kwargs['queryset'] = CelebrityProduct.objects.filter(celebrity_id=celebrity)

        return super(CelebAdmin, self).formfield_for_manytomany(db_field, request, **kwargs)
    '''
    def get_queryset(self, request):
        qs = super(CelebAdmin, self).get_queryset(request)
        if request.user.is_superuser:
            return qs
        return qs.filter(user_name=request.user, created_at__gte = (date.today()-timedelta(1)))



    def get_actions(self, request):
        actions = super(CelebAdmin, self).get_actions(request)
        if 'delete_selected' in actions and not request.user.is_superuser:
            del actions['delete_selected']
        return actions


    list_display = ['user_name','celebrity_name','generic','created_at', 'updated_at']
    search_fields = ['celebrity_name']
    list_filter = ['created_at','updated_at']
    list_per_page = 20

    

class ProductAdmin(admin.ModelAdmin):
    def formfield_for_foreignkey(self, db_field, request, **kwargs):
        if db_field.name == 'user':
            kwargs['queryset'] = get_user_model().objects.filter(username=request.user.username)
        return super(ProductAdmin, self).formfield_for_foreignkey(db_field, request, **kwargs)

    def get_readonly_fields(self, request, obj=None):
        if obj is not None:
            return self.readonly_fields + ('user',)
        return self.readonly_fields

    def add_view(self, request, form_url="", extra_context=None):
        data = request.GET.copy()
        data['user'] = request.user
        request.GET = data
        return super(ProductAdmin, self).add_view(request, form_url="", extra_context=extra_context)
    
    def get_queryset(self, request):
        qs = super(ProductAdmin, self).get_queryset(request)
        if request.user.is_superuser:
            return qs
        return qs.filter(user_name=request.user, created_at__gte = (date.today()-timedelta(1)))

	
    def get_actions(self, request):
        actions = super(ProductAdmin, self).get_actions(request)
        if 'delete_selected' in actions and not request.user.is_superuser:
            del actions['delete_selected']
        return actions

    def get_user(self, obj):
    	return obj.event.user_name

    def get_celebrity(self, obj):
    	return obj.event.celebrity_name

    def get_event_portal(self, obj):
    	return obj.event.event_portal

    def get_event_type(self, obj):
    	return obj.event.event_type

    def get_event_class(self, obj):
    	return obj.event.event_class

    def get_bq_post(self, obj):
    	return obj.event.bq_post

    def get_total_post(self, obj):
    	return obj.event.total_post

    def get_event_date(self, obj):
    	return obj.event.event_date

    def get_event_time(self, obj):
    	return obj.event.event_time

    get_user.admin_order_field = 'event__user_name'
    get_user.short_description = 'User'

    get_celebrity.admin_order_field = 'event__celebrity_name'
    get_celebrity.short_description = 'Celebrity'

    get_celebrity.admin_order_field = 'event__celebrity_name'
    get_celebrity.short_description = 'Celebrity'

    get_event_portal.admin_order_field = 'event__event_portal'
    get_event_portal.short_description = 'Channel'

    get_event_type.admin_order_field = 'event__event_type'
    get_event_type.short_description = 'Type Of Snap'

    get_event_class.admin_order_field = 'event__event_class'
    get_event_class.short_description = 'Content Type'

    get_bq_post.admin_order_field = 'event__bq_post'
    get_bq_post.short_description = 'BQ Post'

    get_total_post.admin_order_field = 'event__total_post'
    get_total_post.short_description = 'Total Post'

    get_event_date.admin_order_field = 'event__event_date'
    get_event_date.short_description = 'Event Date'

    get_event_time.admin_order_field = 'event__event_time'
    get_event_time.short_description = 'Event Time'


    '''
    def queryset(self, request):
        qs = super(ProductAdmin, self).queryset(request)
        return qs.filter(event_id=request.id)
	'''  
    def clean_email(self):
        data = self.cleaned_data['email']
        domain = data.split('@')[1]
        domain_list = ["boutiqaat.com",]
        if domain not in domain_list:
            raise forms.ValidationError["Please enter an Email Address with a valid domain"]
        return data 

    list_display = ['get_user', 'get_celebrity', 'get_event_portal','get_event_type','get_event_class','get_bq_post',
    'get_total_post','get_event_date','get_event_time','skuid','labelid','created_at', 'updated_at']
    #list_display = ['skuid','labelid','created_at', 'updated_at']
    search_fields = ('labelid','skuid')
    list_filter = ['created_at','updated_at']
    list_per_page = 20


admin.site.register(EventsLabelDetails,ProductAdmin)
admin.site.register(Portal)
admin.site.register(EventType)
admin.site.register(EventClass)




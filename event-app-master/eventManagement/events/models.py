from django.db import models
from django.conf import settings
#from django.contrib.auth.models import User



class Portal(models.Model):
    updated_at = models.DateTimeField(db_column='updated_at', auto_now=True)
    portal_name = models.CharField(db_column='portal_name', max_length=20)

    def __str__(self):
        return str(self.portal_name)

    class Meta:
        #managed = False
        db_table = 'portal'
        verbose_name_plural = 'Channel'


class EventType(models.Model):
    updated_at = models.DateTimeField(db_column='updated_at', auto_now=True)
    event_type = models.CharField(db_column='event_type', max_length=20)

    def __str__(self):
        return str(self.event_type)

    class Meta:
        #managed = False
        db_table = 'event_type'
        verbose_name_plural = 'Type Of Snap'


class EventClass(models.Model):
    updated_at = models.DateTimeField(db_column='updated_at', auto_now=True)
    event_class = models.CharField(db_column='event_class', max_length=20)

    def __str__(self):
        return str(self.event_class)

    class Meta:
        #managed = False
        db_table = 'event_class'
        verbose_name_plural = 'Content Type'


class MagentoCelebProd(models.Model):
    created_at = models.DateTimeField()
    celebrity_id = models.IntegerField()
    label = models.IntegerField()
    celebrity_name = models.CharField(max_length=100, blank=True, null=True)
    product_entity_id = models.IntegerField()
    sku = models.CharField(max_length=64, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'magento_celeb_prod'



class EventsHeader(models.Model):
    user_name = models.CharField(max_length=55, blank=True, null=True)
    celebrity_name = models.CharField(max_length=55, null=True)
    celebrity_id = models.PositiveIntegerField(db_column='celebrity_id', null=True)
    generic = models.CharField(max_length=55, null=True)
    event_portal = models.CharField(max_length=50, null=True)
    event_type = models.CharField(max_length=50, null=True)
    event_class = models.CharField(max_length=50, null=True)
    bq_post = models.PositiveIntegerField(db_column='bq_post')
    total_post = models.PositiveIntegerField(db_column='total_post')
    event_date = models.DateField(auto_now=False, auto_now_add=False, blank=True, null=True)
    event_time = models.TimeField(auto_now=False, auto_now_add=False, blank=True, null=True)
    created_at = models.DateTimeField(auto_now=False, auto_now_add=False, blank=True, null=True)
    updated_at = models.DateTimeField(auto_now=False, auto_now_add=False, blank=True, null=True)
    event_hours = models.PositiveIntegerField(blank=True, null=True)
    event_minutes = models.PositiveIntegerField(blank=True, null=True)
    remark = models.TextField(max_length=255,db_column='remark')

    def __str__(self):
        return str(self.user_name) + " | " + str(self.celebrity_name) + " | " + str(self.generic) + " | " + str(self.bq_post)+ " | " + str(self.total_post)+ " | " + str(self.created_at)+ " | " + str(self.updated_at)


    class Meta:
        managed = True
        db_table = 'events_header'
        verbose_name_plural = 'events'

class EventsLabelDetails(models.Model):
	user_name = models.CharField(max_length=55, blank=True, null=True)
	event = models.ForeignKey(EventsHeader, on_delete=models.CASCADE)
	labelid = models.CharField(max_length=50, null=True)
	skuid = models.CharField(max_length=50, null=True)
	created_at = models.DateTimeField(auto_now=False, auto_now_add=False, blank=True, null=True)
	updated_at = models.DateTimeField(auto_now=False, auto_now_add=False, blank=True, null=True)
	
	class Meta:
		managed = True
		db_table = 'events_label_details'
		verbose_name_plural = 'Events'

class ErpSku(models.Model):
    sku = models.CharField(db_column='SKU', primary_key=True, max_length=30)  # Field name made lowercase.
    sku_name = models.CharField(db_column='SKU_NAME', max_length=200, blank=True, null=True)  # Field name made lowercase.
    brand = models.CharField(db_column='BRAND', max_length=50, blank=True, null=True)  # Field name made lowercase.
    category1 = models.CharField(db_column='CATEGORY1', max_length=100, blank=True, null=True)  # Field name made lowercase.
    category2 = models.CharField(db_column='CATEGORY2', max_length=100, blank=True, null=True)  # Field name made lowercase.
    category3 = models.CharField(db_column='CATEGORY3', max_length=40, blank=True, null=True)  # Field name made lowercase.
    category4 = models.CharField(db_column='CATEGORY4', max_length=40, blank=True, null=True)  # Field name made lowercase.

    class Meta:
        managed = False
        db_table = 'ERP_SKU'

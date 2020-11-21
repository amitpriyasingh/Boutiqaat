from prettytable import from_db_cursor
import pymysql
from collections import namedtuple
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import statistics
import pandas as pd
from datetime import datetime
import os
from O365 import *
import json
import base64

def event_tool():
	conn = pymysql.connect(host=os.environ['MYSQL_HOST'], user=os.environ['MYSQL_USER'], passwd=os.environ['MYSQL_PWD'], db='events')
	cursor = conn.cursor()
	print("connected")
	mail_body = ''
	query = ("select h.*,l.skuid, l.labelid from \
(select * from events_header group by user_name,celebrity_name, celebrity_id, \
 generic, event_portal, event_type, event_class, bq_post, total_post, event_date, event_time) as h \
inner join events_label_details l on h.id=l.event_id where date(h.created_at) > subdate(current_date,interval 10 day);")

	table = pd.read_sql(query, con=conn)
	if not table.empty:
		table['created_at_date']=table['created_at'].dt.date
		group_table = table.groupby(['user_name','created_at_date'])
		list_df = []
		df_sku_all = pd.DataFrame({'count(sku)':[0]})
		df_label_all = pd.DataFrame({'count(label)':[0]})
		for i,table in group_table:
			event=0
			total_product=0
			events = len(table['id'].unique())
			user = table['user_name'].unique()
			date = table['created_at_date'].unique()
			group_event = table.groupby(['id','created_at_date'])
			for index, gp_event in group_event:
				event+=int(len(gp_event['id'].unique()))
				celeb_for_sku_replacement = list(gp_event.ix[gp_event['skuid']=='All']['celebrity_id'])
				celeb_for_label_replacement = list(gp_event.ix[gp_event['labelid']=='All']['celebrity_id'])
				if celeb_for_sku_replacement or celeb_for_label_replacement:
					if not celeb_for_sku_replacement:
						celeb_for_sku_replacement='null'
					else:
						celeb_for_sku_replacement = celeb_for_sku_replacement[0]
					if not celeb_for_label_replacement:
						celeb_for_label_replacement = 'null'
					else:
						celeb_for_label_replacement = celeb_for_label_replacement[0]
					count_prod = pd.read_sql("select count(sku) from magento_celeb_prod where celebrity_id = "+str(celeb_for_sku_replacement)+"", con=conn)
					if count_prod.empty:
						count_prod = pd.read_sql("select count(label) from magento_celeb_prod where celebrity_id = "+str(celeb_for_label_replacement)+"", con=conn)
						count_prod = list(count_prod['count(label)'])[0]
					else:
						count_prod = list(count_prod['count(sku)'])[0]
				else:
					total_sku = len(gp_event['skuid'].dropna())
					total_label = len(gp_event['labelid'].dropna())
					count_prod = int(total_sku)+ int(total_label)
				total_product +=count_prod
			#print("=========== Final Result=========",event,total_product,date,user)
			df=pd.DataFrame({'User':user,'Date':date,'Events Count': [event],'Products Count':[total_product]})
			list_df.append(df)
		print(len(list_df))
		if list_df:
			final_df = pd.concat(list_df)
	conn.close()
	print("done")
	mail_body += '<h3> EMT Tool - Last 10 Days Records </h3>'
		
	if not final_df.empty:
		mail_body += final_df.to_html(index=False)
	else:
		mail_body +='<h5> EMT Tool - Data Not Found In Last 10 Days </h5>'

	########################################### Per Day Total Entry ######################################
	mail_body += '<h3> EMT Tool - Datewise Total Entry Of Last 10 Days </h3>'
	gp_final_df = final_df.groupby(['Date'])
	list_perday_rec = []
	for index,df in gp_final_df:
		perday_prod_entry = 0
		perday_event_entry = 0
		date = df['Date'].unique()
		perday_prod_entry = perday_prod_entry + int(df['Products Count'].sum())
		perday_event_entry = perday_event_entry + int(df['Events Count'].sum())
		df_perday_entry = pd.DataFrame({'Date':date,'Products Count':perday_prod_entry,'Events Count':perday_event_entry})
		list_perday_rec.append(df_perday_entry)

	final_df_perday_entry = pd.concat(list_perday_rec)
	final_df_perday_entry['Date'] = pd.to_datetime(final_df_perday_entry['Date'],format="%Y-%m-%d").sort_values()
	final_df_perday_entry['Date'] = final_df_perday_entry['Date'].dt.date
	
	
	avg_prod = statistics.mean(final_df_perday_entry['Products Count'].values.tolist())
	std = statistics.stdev(final_df_perday_entry['Products Count'].values.tolist())
	if not final_df_perday_entry.empty:
		mail_body +=  """<table border='2'>\n<tr>\n<th>Date</th>\n<th>Products Count</th>\n<th>Events Count</th>\n</tr>\n"""
		for row in final_df_perday_entry.values.tolist():
			dev = round(abs(row[2] - avg_prod) / std, 3)
			if dev < 1.3:
				mail_body += """\n<tr>\n<td>""" + str(row[0]) + "</td>\n<td>" + str(row[2]) + "</td>\n<td>" + str(row[1]) + "</td>\n</tr>"
			else:
				mail_body += """\n<tr bgcolor="#FF0000">\n<td>""" + str(row[0]) + "</td>\n<td>" + str(row[2]) + "</td>\n<td>" + str(row[1]) + "</td>\n</tr>"
		mail_body += """</table>"""
	else:
		mail_body +='<h3> Data Not Found  </h3>'
 

	return mail_body
	
		
def events_report():
	conn = pymysql.connect(host=os.environ['MYSQL_HOST'], user=os.environ['MYSQL_USER'], passwd=os.environ['MYSQL_PWD'], db='data_lake')
	cursor = conn.cursor()
	print("connected")
	mail_body = ''
	query = ("select count(distinct event_id), user_name, sum(if(coalesce(productid,labelid) is not NULL,1,0)), date(created_at) from events_report where date(created_at) > subdate(current_date,interval 10 day) group by user_name,date(created_at)")
	table = pd.read_sql(query, con=conn)
	conn.close()
	mail_body += '<h3> Events Report - Last 10 Days Records</h3>'
	if not table.empty:
		table['Total Products'] = table['count(productid)']+table['count(labelid)']
		del table['count(productid)']
		del table['count(labelid)']
		table = table.rename(columns={'created_at':'Date', 'count(distinct event_id)':'Total Events','user_name': 'User'})
		table = table.sort_index(axis=1)
		mail_body += table.to_html(index=False)
		mail_body += '<h3>==========================================</h3>'
		return mail_body
	else:
		mail_body +='<h3> Data Not Found </h3>'
		return mail_body
	



def emt_sync():
	conn = pymysql.connect(host=os.environ['MYSQL_HOST'], user=os.environ['MYSQL_USER'], passwd=os.environ['MYSQL_PWD'], db='events')
	cursor = conn.cursor()
	print("connected")
	mail_body = ''
	mail_body+='<h3>ERP_SKU - Missing SKU</h3>'
	################3 Missing SKU #####################################
	query_missing_sku = "select count(distinct mcp.sku) as Total_Count, group_concat(distinct mcp.sku) as SKU \
						from magento_celeb_prod mcp left join ERP_SKU es on es.sku = mcp.sku where es.sku is null"
	cursor.execute(query_missing_sku)
	missing_sku = cursor.fetchall()
	sku_for_test = cursor.fetchone()
	if sku_for_test is not None:
		total_sku = missing_sku[0][0]
		list_sku = missing_sku[0][1]
		fold = int(len(list_sku)/60)
		fold_dist = 0
		sku=''
		for i in range(0,4):
		    sku += list_sku[fold_dist:fold_dist+60]+'\n'
		    fold_dist+=60
		missing_sku_html_string = '<table border="1">\n<tr>\n<th width="30%">Total SKU</th>\n<th>Missing SKU</th>\n</tr>\n<tr>\n<td align="center">'+str(total_sku)+'</td>\n<td align="center">'+sku+'</td>\n</tr>\n</table>'
		mail_body+=missing_sku_html_string
		mail_body+='<br>'
	else:
		mail_body+='<h5>No Missing Sku</h5>'

	########################################################################################
	mail_body+='<h3>ERP_SKU - Missing Brands</h3>'
	query_missing_brand = ("select count(distinct SKU)as Total_Brands, group_concat(distinct SKU) \
		as Missing_Brands_Of_Sku from ERP_SKU where BRAND is null;")
	cursor.execute(query_missing_brand)
	missing_brands = cursor.fetchall()
	brand_for_test = cursor.fetchone()
	if brand_for_test is not None:
		total_brands = missing_brands[0][0]
		list_sku = missing_brands[0][1]
		fold = int(len(list_sku)/60)
		fold_dist = 0
		brands=''
		for i in range(0,4):
		    brands += list_sku[fold_dist:fold_dist+60]+'\n'
		    fold_dist+=60
		missing_brand_html_string = '<table border="1">\n<tr>\n<th width="30%">Total Brands</th>\n<th>SKU Of Missing Brands</th>\n</tr>\n<tr>\n<td align="center">'+str(total_brands)+'</td>\n<td align="center">'+brands+'</td>\n</tr>\n</table>'
		mail_body += missing_brand_html_string
		mail_body+='<br>'
	else:
		mail_body+='<h5>No Missing Brands</h5><br>'

	conn.close()
	return mail_body


def main():
	report_emt = event_tool()
	report_events = events_report()
	report_emt_sync = emt_sync()
	html_string = report_emt_sync + report_emt + report_events
	authentication = (os.environ['BI_MAIL'], os.environ['BI_MAIL_PWD'])
	m = Message(auth=authentication)
	#m.setRecipients(os.environ['DS_MAIL'])
	m.setRecipients('a.singh@boutiqaat.com')
	m.setSubject('QC Events Report')
	m.setBodyHTML(html_string)
	m.sendMessage()
	print("Mailed")
main()


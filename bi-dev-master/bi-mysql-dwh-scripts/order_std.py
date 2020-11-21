from collections import namedtuple
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import statistics
from sqlalchemy import create_engine
import pandas as pd
import MySQLdb

Constants = namedtuple('Constants', ['username', 'pwd', 'host', 'db', 'from_address', 'to_address', 'smtp_server', 'smtp_username', 'smtp_password'])
constants = Constants('user1', 'pass1', 'master-host-ip', 'db1', 'email1@boutiqaat.com', ['email1@boutiqaat.com'], 'smtp.sendgrid.com', 'user1', 'pass1')
print("seperator ======>")
SEPERATOR = '<h3> ==================================== </h3>'

def query_ofs_slave(query):
    conn = MySQLdb.connect(host='0.0.0.0', user='user1', password='pass1', charset='utf8')
    print("connected To OFS")
    df_ = pd.read_sql(query, conn)
    conn.close()
    return df_
def query_master_ofs(query):
    conn = MySQLdb.connect(host='0.0.0.0', user='user1', password='pass1', charset='utf8')
    print("connected To Master OFS")
    df_ = pd.read_sql(query, conn)
    conn.close()
    return df_

def color_std_red(df):
    color = df[df['Dev.(Factor of SD)']>2]
    x=pd.DataFrame('',index=df.index,columns=df.columns)
    x.loc[color.index,x.columns]='background-color: red'
    return x

def color_std_green(df):
    color_green = df[df['Dev.(Factor of SD)']<2]
    x=pd.DataFrame('',index=df.index,columns=df.columns)
    x.loc[color_green.index,x.columns]='background-color: lime'
   # print(x)
    return x


def order_value_std_factor():
    mail_body = ''
    mail_body += '<h2> Orders Abnormal In last one Month (Since Yesterday) </h2>'
    mail_body += '----------------------------------------'
    query = """select order_date as 'Order Date', order_number as 'Order No',
    sum(quantity) as 'Total Quantity',round(sum(net_sale_price_kwd),2) 'Total Value IN KWD', 
    order_currency as 'Currency',shipping_country as 'Country', payment_method as 'Payment Type'
    from aoi.order_details where order_date between subdate(current_date, interval 31 day) 
    and subdate(current_date, interval 1 day)
    group by order_date, order_number order by order_date desc;"""
    df_=query_ofs_slave(query)
    quantity_std, value_std = df_.std()
    quantity_avg, value_avg = df_.mean()
    df_quantity = df_.loc[:,df_.columns]
    df_value = df_.loc[:,df_.columns]
    df_quantity['Dev.(Factor of SD)'] = df_quantity['Total Quantity'].apply(lambda x: round(abs(x - quantity_avg) / quantity_std, 2))
    df_value['Dev.(Factor of SD)'] = df_value['Total Value IN KWD'].apply(lambda x: round(abs(x - value_avg) / value_std, 2))
    '''
    mail_body += '<h3> Last Day Deviation In Orders </h3>'
    df_last_day_order_deviation = df_quantity[0:1]
    if(df_last_day_order_deviation['Dev.(Factor of SD)'].values[0]<2):
        qty_table = df_last_day_order_deviation.style.apply(color_std_green,axis=None).hide_index()
    else:
        qty_table = df_last_day_order_deviation.style.apply(color_std_red,axis=None).hide_index()
    mail_body+=qty_table.render(axis=1)
    mail_body+=SEPERATOR
    mail_body += '<h3> Last Day Deviation In Value </h3>'
    df_last_day_value_deviation = df_value[0:1]
    if(df_last_day_value_deviation['Dev.(Factor of SD)'].values[0]<2):
        value_table = df_last_day_value_deviation.style.apply(color_std_green,axis=None).hide_index()
    else:
        value_table = df_last_day_value_deviation.style.apply(color_std_red,axis=None).hide_index()
    
    mail_body+=value_table.render(axis=1)
    mail_body+=SEPERATOR
    '''
    mail_body += '<h3> Top 10 Abnormal Deviation In Orders Of Last 30 Days </h3>'
    df_quantity = df_quantity[1:]
    df_quantity = df_quantity.loc[df_quantity['Dev.(Factor of SD)']>2]
    df_qty_top = df_quantity.sort_values(['Dev.(Factor of SD)'],axis=0,ascending=False)
    if len(df_qty_top)>20:
    	df_qty_top = df_qty_top[0:10]
    #top_qty_table = df_qty_top.style.apply(color_std_red,axis=None).hide_index()
    mail_body+=df_qty_top.to_html(index=False)
    mail_body+=SEPERATOR
    mail_body += "<h3> Top 10 Abnormal Deviation Of Order's Values Of Last 30 Days </h3>"
    df_value = df_value[1:]
    df_value = df_value.loc[df_value['Dev.(Factor of SD)']>2]
    df_value_top = df_value.sort_values(['Dev.(Factor of SD)'],axis=0,ascending=False)
    if len(df_value_top)>20:
    	df_value_top = df_value_top[0:10]
    #top_value_table = df_value_top.style.apply(color_std_red,axis=None).hide_index()
    mail_body+=df_value_top.to_html(index=False)
    mail_body+=SEPERATOR
    return mail_body


def mailer():
    report_std_orders = order_value_std_factor()

    html_s = """\
    <html>
      <head><style> 
      table, th, td {{ border: 1px solid black; border-collapse: collapse; }}
      th, td {{ padding: 5px; }}
    </style></head>
      <body>"""
    html_e = """  </body>
    </html>
    """
    body =  html_s + report_std_orders + html_e
    recipients = ['u1@boutiqaat.net','u2@boutiqaat.net']
    msg = MIMEMultipart('mixed')
    msg['Subject'] = "Anomaly Orders Of Last 1 Month"
    msg['From'] = 'bi-u1@boutiqaat.net'
    msg['To'] = ", ".join(recipients)

    msg.attach(MIMEText(body, 'html'))

    server = smtplib.SMTP('smtp.sendgrid.net')
    server.ehlo()
    server.starttls()
    server.login(constants.smtp_username, constants.smtp_password)
    print("here")
    server.sendmail('u1@boutiqaat.net', recipients, msg.as_string())
    server.close()

mailer()

'''
30 05 * * * (cd /opt/anomaly_detection && . venv/bin/activate && python order_std.py && echo 'DONE' && echo `date -u`) > /opt/logs/mailer.log 2>&1
'''

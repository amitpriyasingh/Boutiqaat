from collections import namedtuple
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
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

def query_nav_slave(query):
    engine = create_engine('mssql+pymssql://user1:pass1@host1/db1')
    conn = engine.connect()
    print("connected To Nav")
    df_ = pd.read_sql(query, conn)
    conn.close()
    return df_


def query_bi_app(query):
    conn = MySQLdb.connect(host='0.0.0.0', user='user1', password='pass1', charset='utf8')
    print("connected To BI App")
    df_ = pd.read_sql(query, conn)
    conn.close()
    return df_

def query_to_html_ofs(query):
    list_df=[]
    df_ = query_ofs_slave(query)
    return df_.to_html(index=False)

def query_to_html_nav(query):
    list_df=[]
    df_ = query_nav(query)
    return df_.to_html(index=False)

def query_to_html_bi_app(query):
    list_df=[]
    df_ = query_bi_app(query)
    return df_.to_html(index=False)

def query_to_html_master_ofs(query):
    list_df=[]
    df_ = query_master_ofs(query)
    return df_.to_html(index=False)

def color_std_red(df):
    color = df[df['Dev.(Factor of SD)']>1]
    x=pd.DataFrame('',index=df.index,columns=df.columns)
    x.loc[color.index,x.columns]='background-color: red'
    return x

def color_std_green(df):
    color_green = df[df['Dev.(Factor of SD)']<2]
    x=pd.DataFrame('',index=df.index,columns=df.columns)
    x.loc[color_green.index,x.columns]='background-color: lime'
   # print(x)
    return x

def attach_json(path, cid):
    with open(path, "rb") as image_file:
        encoded_string = base64.b64encode(image_file.read()).decode("utf-8")
    l = """{
      "@odata.type": "#Microsoft.OutlookServices.FileAttachment",
      "Name": "%s",
      "IsInline": true,
      "ContentId": "%s",
      "ContentBytes": "%s"
    }"""% (path, cid, encoded_string)
    return json.loads(l)

def color_distinct_order(df):
    color = df[df['OrdersAOI']!=df['OrdersOFS']]
    x=pd.DataFrame('',index=df.index,columns=df.columns)
    x.loc[color.index,x.columns]='background-color: red'
    return x 

def report_1():
    mail_body = ''

    query = """select date(OrderDateTime) "Date", count(distinct WebOrderNo) "Orders", 
    count(distinct CustomerId) "Customers"  
    from OFS.InboundSalesHeader 
    where date(OrderDateTime) between subdate(now(),interval 10 day) 
    and subdate(now(),interval 1 day) group by 1 order by date(OrderDateTime) desc"""

    df_ = query_ofs_slave(query)
    order_std, cust_std  =  df_.std()
    order_avg, cust_avg = df_.mean()
    df_order = df_.loc[:,['Date', 'Orders']]
    df_cust =  df_.loc[:,['Date', 'Customers']]
    df_cust['Dev.(Factor of SD)'] = df_cust['Customers'].apply(lambda x: round(abs(x - cust_avg) / cust_std, 2))
    df_order['Dev.(Factor of SD)'] = df_order['Orders'].apply(lambda x: round(abs(x - order_avg) / order_std, 2))

    order_table = df_order.style.apply(color_std_red,axis=None).hide_index()
    cust_table = df_cust.style.apply(color_std_red,axis=None).hide_index()

    #order_table = df_order.style.bar(subset=pd.IndexSlice[:,['Dev.(Factor of SD)']], color='#fb1809').render(index=False)
    #cust_table = df_cust.style.bar(subset=pd.IndexSlice[:,['Dev.(Factor of SD)']], color='#fb1809').render(index=False)

    mail_body += '<h3> Distinct Customer Deviation Check </h3>' + cust_table.render(axis=1)
    mail_body += '<h3> Distinct Order Deviation Check </h3>' + order_table.render(axis=1)

    return mail_body

def report_2():
    frame_max_date = []
    mail_body = ''
    mail_body += '<h3> Last Order Dates </h3>'
    query_nav = "select date(date(max([Posting Date]))-1) as NAV from [dbo].[Boutiqaat Kuwait$Sales Invoice Line]"
    query_ofs = "select date(date(max(InsertedOn))-1) as OFS from OFS.InboundSalesHeader"
    query_aoi = "select date(date(max(order_inserted_on_utc))-1) as AOI from aoi.order_details"
    df_ofs = query_ofs_slave(query_ofs)
    print(df_ofs)
    frame_max_date.append(df_ofs)
    try:
        df_nav = query_nav_slave(query_nav)
        print(df_nav)
        frame_max_date.append(df_nav)
    except:
        print("connection Error To Nav DB")
    df_aoi = query_ofs_slave(query_aoi)
    print(df_aoi)
    frame_max_date.append(df_aoi) 
    print(frame_max_date)
    df_max_date = pd.concat(frame_max_date,axis=1)
    print(df_max_date)
    mail_body += df_max_date.to_html(index=False)
    #mail_body += SEPERATOR
    print(" End of end date query")
    ################### end date of max date orders report #################
    mail_body += '<h3> Distinct Orders </h3>'

    query_distinct_orders = "select aoi.order_date Date,\
                aoi.orders OrdersAOI,\
                ish.orders OrdersOFS from \
                (select count(distinct order_number) orders ,date(order_inserted_on_utc) order_date \
                    from aoi.order_details where date(order_inserted_on_utc) between subdate(now(),interval 10 day) and subdate(now(),interval 1 day) \
                group by 2) aoi \
                left join (select count(distinct WebOrderNo) orders ,\
                date(InsertedOn) dt \
                from OFS.InboundSalesHeader where date(InsertedOn) between subdate(now(),interval 10 day) and subdate(now(),interval 1 day) \
                group by 2) ish \
                on aoi.order_date = ish.dt order by aoi.order_date desc;"

    df_distinct_order = query_ofs_slave(query_distinct_orders) 
    distinct_order_table = df_distinct_order.style.apply(color_distinct_order,axis=None).hide_index()
    mail_body += distinct_order_table.render(axis=1)
    #mail_body += SEPERATOR

    mail_body += '<h3> aoi - Missing Values </h3>'
    query_missing_value = "select date(o.order_inserted_on_utc) Date,\
        sum(case when o.category1 is NULL then 1 else 0 end) as Category1,\
        sum(case when o.category2 is NULL then 1 else 0 end) as Category2,\
        sum(case when o.brand is NULL then 1 else 0 end) as Brand,\
        sum(case when o.sku is NULL then 1 else 0 end) as SKU,\
        sum(case when o.sku_name is NULL then 1 else 0 end) as SKUName,\
        sum(case when o.order_number is NULL then 1 else 0 end) as OrderNo,\
        sum(case when o.customer_id is NULL then 1 else 0 end) as CustomerID,\
        sum(case when o.celebrity_name is NULL then 1 else 0 end) as Celebrity,\
        sum(case when o.order_status is NULL then 1 else 0 end) as Status,\
        sum(case when o.payment_method is NULL then 1 else 0 end) as PaymentMethod,\
        sum(case when o.shipping_country is NULL then 1 else 0 end) as Country,\
        sum(case when a.city is NULL then 1 else 0 end) as City,\
        sum(case when time(o.order_at) is NULL then 1 else 0 end) as Order_Time,\
        sum(case when o.shipping_phone_no is NULL and o.shipping_country <> 'Celebrities' then 1 else 0 end) as CustTel,\
        sum(case when o.net_sale_price_kwd is NULL then 1 else 0 end) as SaleValue \
        from aoi.order_details o \
        left join (select WebOrderNo,City from OFS.InboundOrderAddress group by WebOrderNo) a on a.WebOrderNo = o.order_number \
        where date(o.order_inserted_on_utc) >= subdate(current_date, interval 10 day) and date(o.order_inserted_on_utc) < current_date group by 1 order by date(o.order_inserted_on_utc) desc;"

    mail_body += query_to_html_ofs(query_missing_value)
    #mail_body += SEPERATOR
    '''
    mail_body += '<h3> COGS QC</h3>'
    query_cogs = """select order_inserted_on_utc "Date",  
    sum(case when s.cost_price<=0 then 1 else 0 end ) "COGS<=0", 
    sum(case when s.cost_price>=o.net_sale_price_kwd then 1 else 0 end) "COGS>=Price", 
    sum(case when o.quantity<1 then 1 else 0 end) "Qty<1",  
    sum(case when o.net_sale_price_kwd<0 then 1 else 0 end) "Price<1",  
    sum(case when o.net_sale_price_kwd=0 and order_category <> 'CELEBRITY' then 1 else 0 end) "Price=0(Non-Celeb)",  
    count(distinct case when shipping_charge<=0 or shipping_charge is NULL then order_number end) "Shipping<=0" 
    from aoi.order_details o 
    left join aoi.soh_report s on s.sku = o.sku
    where o.order_inserted_on_utc >= current_date - 10 group by 1 order by order_inserted_on_utc desc;"""

    mail_body += query_to_html_ofs(query_cogs)
    mail_body += SEPERATOR
    '''
    mail_body += '<h3>Price Variation-SKU</h3>'
    query_price_validation = """select count(distinct case when a.min_ < 0.8*a.max_ then a.sku end) as "Variation in Price >20percent" \
            from \
            (select min(net_sale_price_kwd) min_, max(net_sale_price_kwd) max_, sku \
                from aoi.order_details where date(order_inserted_on_utc) >= current_date - 10 and date(order_inserted_on_utc) < current_date group by sku) a"""
    
    mail_body += query_to_html_ofs(query_price_validation)
    #mail_body += SEPERATOR

    mail_body += '<h3> Visibility Flag Availability </h3>'
    
    mail_body += '<h5> Magento Table </h5>'
    query_visibility_magento = """select \
            count(distinct case when c1.visibility in (4,3) then c1.sku end ) "Visible", \
            count(distinct case when c1.visibility=1 then c1.sku end) "Not Visible", \
            count(distinct case when c1.visibility is NULL then c1.sku end) "Blank" \
            from (select distinct sku from data_lake.order_details group by 1) i \
            left join data_lake.catalog_product_flat_1 c1 on c1.sku = i.sku"""
    mail_body += query_to_html_bi_app(query_visibility_magento)
    #mail_body += SEPERATOR

    return mail_body


def report_5():
    mail_body = ''

    mail_body += '<h3> events_report - Missing Values</h3>'
    query = """select event_date "Date",  
    sum( case when account_manager is NULL then 1 else 0 end) "Account Manager", 
    sum( case when celebrity_name is NULL then 1 else 0 end) "Celebrity Name", 
    sum( case when celebrity_id is NULL then 1 else 0 end) "Celebrity ID", 
    sum( case when sku_code is NULL then 1 else 0 end) "SKUCode", 
    sum( case when category1 is NULL then 1 else 0 end) "Category1", 
    sum( case when purchase_type is NULL then 1 else 0 end) "PurchaseType", 
    sum( case when brand is NULL then 1 else 0 end) "Brand" 
    from (select * from data_lake.events_report where event_date 
    between subdate(now(),interval 10 day) and subdate(now(),interval 1 day)) a group by event_date order by event_date desc; """
    
    df_ = query_bi_app(query)
    mail_body += df_.to_html(index=False)
    #mail_body += SEPERATOR

    if df_.shape[0] == 0:
        mail_body =''

    return mail_body

def report_6():
    mail_body=''
    mail_body += '<h3> Missing Items For Orders </h3>'
    query = """ select date(InsertedOn) as 'Date', count(ItemId) as 'Missing ItemID' from OFS.InboundSalesLine
         where ItemId is NULL or ItemId = 0 group by date(InsertedOn)
         order by date(InsertedOn) desc limit 1;"""
    df_ = query_ofs_slave(query)
    if df_.empty:
        mail_body = ''
    else:
        mail_body += df_.to_html(index=False)
    return mail_body

def report_7():
    mail_body=''
    mail_body += '<h3> Distinct Customer Cohort Report </h3>'
    query = """ select date(order_date) date, count(distinct customer_telephone) customers, 
            count(distinct if(is_first_order=1,customer_telephone, NULL)) new_customers,
            concat(round((count(distinct customer_telephone) - count(distinct if(is_first_order=1,customer_telephone, NULL)))*100/count(distinct customer_telephone),2),'%') 'Repeated Customer'
            from data_lake.aoi where date(order_date) between subdate(now(), interval 10 day) 
            and subdate(now(), interval 1 day) group by date(order_date) order by date desc"""
    mail_body += query_to_html_bi_app(query)
    return mail_body

def traffic_report():
    mail_body=''
    mail_body += '<h3> Traffic Report </h3>'
    query = """select date as 'Date', sum(sessions) sessions, sum(users) visitors,sum(screen_views) screen_views 
    from ga.traffic_datewise where date between subdate(current_date, interval 8 day) 
    and subdate(current_date, interval 1 day) group by date order by date desc;"""
    df_= query_bi_app(query)
    sessions_std, visitors_std, screen_views_std =df_.std()
    sessions_mean, visitors_mean, screen_views_mean =df_.mean()
    df_sessions = df_.loc[:,['Date','sessions']]
    df_visitors = df_.loc[:,['Date','visitors']]
    df_screen_views = df_.loc[:,['Date','screen_views']]
    df_sessions['Dev.(Factor of SD)'] = df_sessions['sessions'].apply(lambda x: round(abs(x - sessions_mean) / sessions_std, 2))
    df_visitors['Dev.(Factor of SD)'] = df_visitors['visitors'].apply(lambda x: round(abs(x - visitors_mean) / visitors_std, 2))
    df_screen_views['Dev.(Factor of SD)'] = df_screen_views['screen_views'].apply(lambda x: round(abs(x - screen_views_mean) / screen_views_std, 2))
    sessions_table = df_sessions.style.apply(color_std_red,axis=None).hide_index()
    visitors_table = df_visitors.style.apply(color_std_red,axis=None).hide_index()
    screen_views_table = df_screen_views.style.apply(color_std_red,axis=None).hide_index()
    mail_body += '<h3> Unique Visitor Deviation Check </h3>' + visitors_table.render(axis=1)
    mail_body += '<h3> Distinct Session Deviation Check </h3>' + sessions_table.render(axis=1)
    mail_body += '<h3> Distinct ScreenViews Deviation Check </h3>' + screen_views_table.render(axis=1)
    mail_body += '<p>**Includes visitors, session and screenviews on all pages.\n</p>'
    return mail_body

def master_ofs_order_status():
    mail_body=''
    mail_body += '<h3> Last 7 Days Sync Of Master Order Status & OFS Slave Order Status </h3>'
    query = """select date(o.InsertedOn) OrderDate, o.ItemId ItemID, o.StatusId StatusID, m.StatusName StatusName
    from OFS.OrderStatus o
    inner join OFS.StatusMaster m
    on m.ID = o.StatusId
    where date(o.InsertedOn) between subdate(current_date,interval 7 day)
    and subdate(current_date,interval 1 day) group by date(o.InsertedOn),o.ItemId,o.StatusId order by date(o.InsertedOn) desc"""

    df_master = query_master_ofs(query)
    df_slave = query_ofs_slave(query)
    mail_body+='<h3> Total Items In OFS Master & OFS SLave</h3>'
    total_item_master = df_master['ItemID'].groupby(df_master['OrderDate']).count()
    total_item_slave = df_slave['ItemID'].groupby(df_slave['OrderDate']).count()
    df_m = pd.DataFrame(total_item_master)
    df_s = pd.DataFrame(total_item_slave)
    df_m.rename(columns={'ItemID':'Total Item Id Of Master'}, inplace=True)
    df_s.rename(columns={'ItemID':'Total Item Id Of Slave'}, inplace=True)
    df_count = df_m.join(df_s).sort_values(['OrderDate'],ascending=False)
    mail_body+=df_count.to_html()+SEPERATOR
    mail_body+='<h3> Mismatch Status In OFS Master & OFS SLave</h3>'
    df_master=df_master.set_index(df_master['OrderDate'].map(str)+'_'+df_master['ItemID'].map(str)+'_'+df_master['StatusID'].map(str))
    df_slave=df_slave.set_index(df_slave['OrderDate'].map(str)+'_'+df_slave['ItemID'].map(str)+'_'+df_slave['StatusID'].map(str))
    df_master.rename(columns={'OrderDate':'Order Date','StatusID':'Status ID Master','StatusName':'Status Name Master'},inplace=True)
    df_slave.rename(columns={'StatusID':'Status ID Slave','StatusName':'Status Name Slave'},inplace=True)
    df_final_m = df_master[['Order Date','Status ID Master','Status Name Master']]
    df_final_s = df_slave[['OrderDate','Status ID Slave','Status Name Slave']]
    df_final=df_final_m.join(df_final_s, how='outer')

    def f(x):
        return x.split('_')[1]
    df_final['Item ID']=df_final.index.to_series().apply(f)

    df_final['Match']=df_final[['Status ID Master','Status ID Slave']].apply(lambda x:'Matched' if x[0]==x[1] else 'Mismatched',axis=1)
    df_final=df_final[['Order Date','Item ID','Status Name Master','Status Name Slave','Match']]
    df_mismatched = df_final = df_final.loc[df_final['Match']=='Mismatched']
    if len(df_mismatched)>0:
        mail_body+=df_mismatched.to_html(index=False)+SEPERATOR
    else:
        mail_body+='<p style="color:green;">All Order Status Of Master Table is matched with OFS Table </p>'
    
    if len(df_final[df_final['Match']=='Mismatched'])>0:
        mail_body+=df_final[df_final['Match']=='Mismatched'].to_html(index=False)+SEPERATOR
    return mail_body

def abnormal_pick_deliver_date():
    mail_body=''
    mail_body += '<h3> Differences - Order Date Vs. Picking Date, Order Date Vs Shipping Date & \
    Order Date Vs Delivery Date - Order Neither On Hold Nor Cancel </h3>'
    query ="""select 
    count(case when abs((date(order_inserted_on_utc) - date(picked_at)))>=5 and  abs((date(order_inserted_on_utc) - date(picked_at)))<=15 then item_id else NULL end) p1,
    count(case when abs((date(order_inserted_on_utc) - date(picked_at)))>15 and  abs((date(order_inserted_on_utc) - date(picked_at)))<=30 then item_id else NULL end) p2,
    count(case when abs((date(order_inserted_on_utc) - date(picked_at)))>30 and  abs((date(order_inserted_on_utc) - date(picked_at)))<=60 then item_id else NULL end) p3,
    count(case when abs((date(order_inserted_on_utc) - date(picked_at)))>60 and  abs((date(order_inserted_on_utc) - date(picked_at)))<=100 then item_id else NULL end) p4,
    count(case when abs((date(order_inserted_on_utc) - date(picked_at)))>100 then item_id else NULL end) p5,

    count(case when abs((date(order_inserted_on_utc) - date(shipped_at)))>=5 and  abs((date(order_inserted_on_utc) - date(shipped_at)))<=15 then item_id else NULL end) s1,
    count(case when abs((date(order_inserted_on_utc) - date(shipped_at)))>15 and  abs((date(order_inserted_on_utc) - date(shipped_at)))<=30 then item_id else NULL end) s2,
    count(case when abs((date(order_inserted_on_utc) - date(shipped_at)))>30 and  abs((date(order_inserted_on_utc) - date(shipped_at)))<=60 then item_id else NULL end) s3,
    count(case when abs((date(order_inserted_on_utc) - date(shipped_at)))>60 and  abs((date(order_inserted_on_utc) - date(shipped_at)))<=100 then item_id else NULL end) s4,
    count(case when abs((date(order_inserted_on_utc) - date(shipped_at)))>100 then item_id else NULL end) s5,

    count(case when abs((date(order_inserted_on_utc) - date(delivered_at)))>=5 and  abs((date(order_inserted_on_utc) - date(delivered_at)))<=15 then item_id else NULL end) d1,
    count(case when abs((date(order_inserted_on_utc) - date(delivered_at)))>15 and  abs((date(order_inserted_on_utc) - date(delivered_at)))<=30 then item_id else NULL end) d2,
    count(case when abs((date(order_inserted_on_utc) - date(delivered_at)))>30 and  abs((date(order_inserted_on_utc) - date(delivered_at)))<=60 then item_id else NULL end) d3,
    count(case when abs((date(order_inserted_on_utc) - date(delivered_at)))>60 and  abs((date(order_inserted_on_utc) - date(delivered_at)))<=100 then item_id else NULL end) d4,
    count(case when abs((date(order_inserted_on_utc) - date(delivered_at)))>100 then item_id else NULL end) d5
    from aoi.order_details where date(order_inserted_on_utc)<subdate(current_date,interval 7 day)
    and order_status_id not in (10,11)
    order by date(order_inserted_on_utc);"""
    df_diff_date = query_ofs_slave(query)
    arr_diff = df_diff_date.values.tolist()
    df_diff = pd.DataFrame({'05-15 Days Diff':[arr_diff[0][0],arr_diff[0][5],arr_diff[0][10]],'15-30 Days Diff':[arr_diff[0][1],arr_diff[0][6],arr_diff[0][11]],'30-60 Days Diff':[arr_diff[0][2],arr_diff[0][7],arr_diff[0][12]],'60-100 Days Diff':[arr_diff[0][3],arr_diff[0][8],arr_diff[0][13],],'>100 Days Diff':[arr_diff[0][4],arr_diff[0][9],arr_diff[0][14]]},index=['Picked','Shiped','Delivered'])
    mail_body+=df_diff.to_html()+ SEPERATOR

    mail_body += '<h3> Ordered Happend & Order Neither On Hold Nor Cancel But Missing Picked Date, Shipped Date & Delivered Date </h3>'
    query_missing = """select 
        count(case when picked_at is null and date(order_inserted_on_utc)< subdate(current_date,interval 7 day) 
            and date(order_inserted_on_utc) >= subdate(current_date,interval 21 day) then item_id else NULL end) pn15d,
        count(case when picked_at is null and date(order_inserted_on_utc)< subdate(current_date,interval 21 day) 
            and date(order_inserted_on_utc) >= subdate(current_date,interval 37 day) then item_id else NULL end) pn1m,
        count(case when picked_at is null and date(order_inserted_on_utc)< subdate(current_date,interval 37 day) 
            and date(order_inserted_on_utc) >= subdate(current_date,interval 187 day) then item_id else NULL end) pn6m,
        count(case when picked_at is null and date(order_inserted_on_utc)< subdate(current_date,interval 187 day) 
            and date(order_inserted_on_utc) >= subdate(current_date,interval 372 day) then item_id else NULL end) pn1y,
        count(case when picked_at is null and date(order_inserted_on_utc)< subdate(current_date,interval 372 day) 
            then item_id else NULL end) pny,

        count(case when shipped_at is null and date(order_inserted_on_utc)< subdate(current_date,interval 7 day) 
            and date(order_inserted_on_utc) >= subdate(current_date,interval 21 day) then item_id else NULL end) sn15d,
        count(case when shipped_at is null and date(order_inserted_on_utc)< subdate(current_date,interval 21 day) 
            and date(order_inserted_on_utc) >= subdate(current_date,interval 37 day) then item_id else NULL end) sn1m,
        count(case when shipped_at is null and date(order_inserted_on_utc)< subdate(current_date,interval 37 day) 
            and date(order_inserted_on_utc) >= subdate(current_date,interval 187 day) then item_id else NULL end) sn6m,
        count(case when shipped_at is null and date(order_inserted_on_utc)< subdate(current_date,interval 187 day) 
            and date(order_inserted_on_utc) >= subdate(current_date,interval 372 day) then item_id else NULL end) sn1y,
        count(case when shipped_at is null and date(order_inserted_on_utc)< subdate(current_date,interval 372 day) 
            then item_id else NULL end) sny,

        count(case when delivered_at is null and date(order_inserted_on_utc)< subdate(current_date,interval 7 day) 
            and date(order_inserted_on_utc) >= subdate(current_date,interval 21 day) then item_id else NULL end) pn15d,
        count(case when delivered_at is null and date(order_inserted_on_utc)< subdate(current_date,interval 21 day) 
            and date(order_inserted_on_utc) >= subdate(current_date,interval 37 day) then item_id else NULL end) pn1m,
        count(case when delivered_at is null and date(order_inserted_on_utc)< subdate(current_date,interval 37 day) 
            and date(order_inserted_on_utc) >= subdate(current_date,interval 187 day) then item_id else NULL end) pn6m,
        count(case when delivered_at is null and date(order_inserted_on_utc)< subdate(current_date,interval 187 day) 
            and date(order_inserted_on_utc) >= subdate(current_date,interval 372 day) then item_id else NULL end) pn1y,
        count(case when delivered_at is null and date(order_inserted_on_utc)< subdate(current_date,interval 372 day) 
            then item_id else NULL end) pny
        from aoi.order_details
        where order_status_id not in (10,11);"""

    df_missing = query_ofs_slave(query_missing)
    arr_missing = df_missing.values.tolist()
    df_miss = pd.DataFrame({'05-15 Days Diff':[arr_missing[0][0],arr_missing[0][5],arr_missing[0][10]],'15-30 Days Diff':[arr_missing[0][1],arr_missing[0][6],arr_missing[0][11]],'30-60 Days Diff':[arr_missing[0][2],arr_missing[0][7],arr_missing[0][12]],'60-100 Days Diff':[arr_missing[0][3],arr_missing[0][8],arr_missing[0][13],],'>100 Days Diff':[arr_missing[0][4],arr_missing[0][9],arr_missing[0][14]]},index=['Picked','Shiped','Delivered'])
    mail_body+= df_miss.to_html() + SEPERATOR

    return mail_body

def mailer():
    report_body1 = report_1()
    report_body2 = report_2()
    report_body5 = report_5()
    report_body6 = report_6()
    report_body7 = report_7()
    report_traffic = traffic_report()
    master_ofs_qc = master_ofs_order_status()
    report_psd = abnormal_pick_deliver_date()
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
    	
    body = html_s + report_body1 + report_body2 + report_body5 + report_body6 + report_body7 + report_traffic + master_ofs_qc + report_psd + html_e
    #body =  html_s + report_body1 + html_e
    recipients = ['u1@boutiqaat.net','u2@boutiqaat.net']
    msg = MIMEMultipart('mixed')
    msg['Subject'] = "DQ Report Developed On New System"
    msg['From'] = 'bi-u1@boutiqaat.com'
    msg['To'] = ", ".join(recipients)
    msg.attach(MIMEText(body, 'html'))
    server = smtplib.SMTP('smtp.sendgrid.net')
    server.ehlo()
    server.starttls()
    server.login(constants.smtp_username, constants.smtp_password)
    print("here")
    server.sendmail('u2@boutiqaat.net', recipients, msg.as_string())
    server.close()

mailer()


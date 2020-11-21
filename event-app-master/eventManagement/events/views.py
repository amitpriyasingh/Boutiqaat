from django.shortcuts import render
from django.shortcuts import render_to_response, get_object_or_404
from .models import *
import pymysql
from django.contrib.auth import get_user_model
from django.http import HttpResponse, HttpResponseRedirect
from django.urls import reverse
import json
import pandas as pd
import datetime

#from .forms import CelebrityProductForm



def welcome(request):
    return HttpResponseRedirect(request, '/admin/')


def goback(request):
    # your code
    return HttpResponseRedirect(reverse('celebrity_mapping'))


def getproductid(request):
    celebrity = request.GET['celebrity']
    list_data = []
    print('=======Celebrity=======',celebrity)
    celebrity_id = MagentoCelebProd.objects.filter(celebrity_name=celebrity).values('celebrity_id').distinct()
    celebrity_id = celebrity_id[0]['celebrity_id']
    product_id = MagentoCelebProd.objects.filter(celebrity_id=celebrity_id).values('sku').distinct()
    list_product_id = [i for i in product_id]
    df_sku = pd.DataFrame(list_product_id)
    if df_sku.empty:
        list_id=[]
    else:
        list_id = df_sku['sku'].values.tolist()
        list_id = sorted(list_id,reverse=True)
    #print(list_id)
    list_data.append(list_id)
    label = MagentoCelebProd.objects.filter(celebrity_id=celebrity_id).values('label').distinct()
    list_label = [i for i in label]
    df_label = pd.DataFrame(list_label)
    print(df_label)
    if df_label.empty:
        lst_label=[]
    else:
        lst_label = df_label['label'].values.tolist()
        lst_label = sorted(lst_label,reverse=True)
    #
    print(lst_label)
    json_data = {'list_id':list_id,'list_label':lst_label}
    list_data.append(lst_label)
    print(list_data)
    return HttpResponse(json.dumps(list_data), content_type='application/json')   

def celebrity_mapping(request):
    user = get_user_model().objects.get(username=request.user.username)
    print(user)
    
    #celebrity = Celebrity.objects.values_list('name_e',flat=True).distinct()
    #celebrity = Celebrity.objects.all()

    celebrity = list(MagentoCelebProd.objects.values_list('celebrity_name',flat=True).distinct())
    celebrity = sorted(celebrity)
    portal = list(Portal.objects.values_list('portal_name',flat=True).distinct())
    event_type = list(EventType.objects.values_list('event_type',flat=True).distinct())
    eventclass = list(EventClass.objects.values_list('event_class',flat=True).distinct())
    context ={'user':user,'celebrity':celebrity,'eventportal':portal,'event_type':event_type,'eventclass':eventclass}

    return render(request,'celebrity_mapping/celebrity_product.html',context)


def preview(request):
    user = request.POST.get("user") 
    celebrity = request.POST.get("celebrityname",'')
    generic = request.POST.get("genericname",'') 
    productid = request.POST.getlist("productidname",'')
    labelid = request.POST.getlist("labelname",'')  
    eventportal = request.POST.get("eventportalname",'')
    eventtype = request.POST.get("eventtypename",'') 
    eventclass = request.POST.get("eventclassname",'')
    totalpost = request.POST.get("total_post",'') 
    bqpost = request.POST.get("bq_post",'')
    eventdate = request.POST.get("date",'')
    eventtime = request.POST.get("time",'')
    remark = request.POST.get("remark",'')

    celebrity_id = MagentoCelebProd.objects.filter(celebrity_name=celebrity).values('celebrity_id').distinct()
    celebrity_id = celebrity_id[0]['celebrity_id']
    print("=============Length Of Products and Labels==============")
    print(len(productid))
    print(len(labelid))
    print(user,celebrity,generic,productid,eventportal,eventtype,eventclass,totalpost,bqpost,remark,eventdate,eventtime)


    if eventportal !='null':
        eventportal=eventportal.strip('[]').replace("'",'')
    if eventtype != 'null':
        eventtype=eventtype.strip('[]').replace("'",'')
        print('========',eventtype)
    if eventclass != 'null':
        eventclass=eventclass.strip('[]').replace("'",'')
    #if generic == 'Yes':
        #productid = 'All'
    if not productid:
        productid=None
   
    if not labelid:
        labelid=None

    if productid:
        listBrandName=[]
        listCategory1=[]
        listCategory2=[]   
        for p in productid:
            brandName = ErpSku.objects.filter(sku=p).values('brand')
            if brandName:
                brandName=brandName[0]
                b=list(brandName.values())[0]
                listBrandName.append(b)
            else:
                listBrandName.append('Not Found')
            cat1 = ErpSku.objects.filter(sku=p).values('category1')
            if cat1:
                cat1=cat1[0]
                c1 = list(cat1.values())[0]
                listCategory1.append(c1)
            else:
                listCategory1.append('Not Found')
            cat2 = ErpSku.objects.filter(sku=p).values('category2')
            if cat2:
                cat2=cat2[0]
                c2 = list(cat2.values())[0]
                listCategory2.append(c2)
            else:
                listCategory2.append('Not Found')
        print("========== Printing Brand & Category For Product ID==============")
        print(listBrandName)
        print(listCategory1)
        print(listCategory2)
        #dictCelebrityPreview={'Celebrity Name':celebrity}
        dictProductIdPreview={'Product ID':productid}
        dfProductIdPreview=pd.DataFrame(dictProductIdPreview)
        dfProductIdPreview['Brand Name']=listBrandName
        dfProductIdPreview['Category1']=listCategory1
        dfProductIdPreview['Category2']=listCategory2
        dfProductIdPreview['User']=user
        dfProductIdPreview['Celebrity Name']=celebrity
        dfProductIdPreview['Generic']=generic
        dfProductIdPreview['Event Portal']=eventportal
        dfProductIdPreview['Event Type']=eventtype
        dfProductIdPreview['Event Class']=eventclass
        dfProductIdPreview['Total Post']=totalpost
        dfProductIdPreview['BQ Post']=bqpost
        dfProductIdPreview['Event Date']=eventdate
        dfProductIdPreview['Event Time']=eventtime
        dfProductIdPreview['Remark']=remark
        dfProductIdPreview['Label ID']=None
    else:
        dfProductIdPreview=pd.DataFrame()
    
    if labelid:
        listBrandName=[]
        listCategory1=[]
        listCategory2=[]
        for l in labelid:
            try:
                sku = MagentoCelebProd.objects.filter(label=l).filter(celebrity_id=celebrity_id).values('sku')[0]
            except:
                sku = None
            try:
                for k,v in sku.items():
                    s = v
            except:
                s = None
            #s = list(sku.values())[0]
            brandName = ErpSku.objects.filter(sku=s).values('brand')
            if brandName:
                brandName=brandName[0]
                b=list(brandName.values())[0]
                listBrandName.append(b)
            else:
                listBrandName.append('Not Found')
            cat1 = ErpSku.objects.filter(sku=s).values('category1')
            if cat1:
                cat1=cat1[0]
                c1 = list(cat1.values())[0]
                listCategory1.append(c1)
            else:
                listCategory1.append('Not Found')
            cat2 = ErpSku.objects.filter(sku=s).values('category2')
            if cat2:
                cat2=cat2[0]
                c2 = list(cat2.values())[0]
                listCategory2.append(c2)
            else:
                listCategory2.append('Not Found')
        print("========== Printing Brand & Category For Lable==============")
        print(listBrandName)
        print(listCategory1)
        print(listCategory2)
        dictLabelIdPreview={'Label ID':labelid}
        dfLabelIdPreview=pd.DataFrame(dictLabelIdPreview)
        dfLabelIdPreview['Brand Name']=listBrandName
        dfLabelIdPreview['Category1']=listCategory1
        dfLabelIdPreview['Category2']=listCategory2
        dfLabelIdPreview['User']=user
        dfLabelIdPreview['Celebrity Name']=celebrity
        dfLabelIdPreview['Generic']=generic
        dfLabelIdPreview['Event Portal']=eventportal
        dfLabelIdPreview['Event Type']=eventtype
        dfLabelIdPreview['Event Class']=eventclass
        dfLabelIdPreview['Total Post']=totalpost
        dfLabelIdPreview['BQ Post']=bqpost
        dfLabelIdPreview['Event Date']=eventdate
        dfLabelIdPreview['Event Time']=eventtime
        dfLabelIdPreview['Remark']=remark
        dfLabelIdPreview['Product ID']=None
    else:
        dfLabelIdPreview=pd.DataFrame()


    allProductId = MagentoCelebProd.objects.filter(celebrity_id=celebrity_id).values('sku').distinct()
    allLabelId = MagentoCelebProd.objects.filter(celebrity_id=celebrity_id).values('label').distinct()
    #print(len(allLabelId),len(labelid))
    #print(len(allProductId),len(productid))
    if productid:
        if len(allProductId)==len(productid):
            productid = 'All'
            listBrandName = 'All'
            listCategory1 = 'All'
            listCategory2 = 'All'
            dfProductIdPreview['Product ID']=productid
            dfProductIdPreview['Brand Name']=listBrandName
            dfProductIdPreview['Category1']=listCategory1
            dfProductIdPreview['Category2']=listCategory2
            dfProductIdPreview = dfProductIdPreview.iloc[:1,:]
    if labelid:
        if len(allLabelId)==len(labelid):
            labelid='All'
            listBrandName = 'All'
            listCategory1 = 'All'
            listCategory2 = 'All'
            dfLabelIdPreview['Label ID']=labelid
            dfLabelIdPreview['Brand Name']=listBrandName
            dfLabelIdPreview['Category1']=listCategory1
            dfLabelIdPreview['Category2']=listCategory2
            dfLabelIdPreview = dfLabelIdPreview.iloc[:1,:]

    dfFinal=dfProductIdPreview.append(dfLabelIdPreview, ignore_index=True)
    dfFinal=dfFinal[sorted(dfFinal.columns)]
    print(dfFinal.columns)
    listData=dfFinal.values.tolist()
    
    context={'listData':listData}
    home = request.POST.get("homename",'')
    preview = request.POST.get("previewname",'')
    print("================Home/Preview===================")
    print(home)
    print(preview)
    if home == 'Home':
        return HttpResponseRedirect('/admin/')
    if preview == 'Preview':
        return render(request, 'celebrity_mapping/preview.html',context)

def success(request):
    user = request.POST.get("user") 
    celebrity = request.POST.get("celebrityname",'')
    generic = request.POST.get("genericname",'') 
    productid = request.POST.getlist("productidname",'') 
    labelid= request.POST.getlist("labelidname",'')
    eventportal = request.POST.get("eventportalname",'')
    eventtype = request.POST.get("eventtypename",'') 
    eventclass = request.POST.get("eventclassname",'')
    totalpost = request.POST.get("total_post",'') 
    bqpost = request.POST.get("bq_post",'')
    eventdate = request.POST.get("eventdate",'')
    eventtime = request.POST.get("eventtime",'')
    remark = request.POST.get("remark",'')
    saveadd = request.POST.get("saveandadd",'')
    savehome = request.POST.get("saveandhome",'')
    
    #data=list(data)
    print("========= Data For Insert ================")
    print(user,celebrity,generic,productid,eventportal,eventtype,eventclass,
        totalpost,bqpost,remark,eventdate,eventtime)
    celebrity_id = MagentoCelebProd.objects.filter(celebrity_name=celebrity).values('celebrity_id').distinct()
    celebrity_id = celebrity_id[0]['celebrity_id']

    se=EventsHeader(user_name = user,
        celebrity_name=celebrity,
        celebrity_id = celebrity_id,
        generic= generic,
        event_portal= eventportal,
        event_type= eventtype,
        event_class= eventclass,
        total_post=totalpost,
        bq_post= bqpost,
        event_date= eventdate,
        event_time= eventtime,
        created_at=datetime.datetime.now(),
        updated_at=None,
        remark= remark)
    se.save()
    event_id = se.id
    created_at= se.created_at

    dictProductIdPreview={'ProductID':productid}
    dfForInsert=pd.DataFrame(dictProductIdPreview)
    dfForInsert['Label'] = labelid
    dfForInsert['EventID'] = event_id
    dfForInsert['CreateAt'] = created_at

    dfFinal=dfForInsert[sorted(dfForInsert.columns)]
    print(dfFinal)
    listData=dfFinal.values.tolist()

    for i in listData:
        if(i[3]=='None'):
            skuid=None
        else:
            skuid=i[3]

        if(i[2]=='None'):
            labelid=None
        else:
            labelid=i[2]

        sd=EventsLabelDetails(
            user_name = user,
            labelid=labelid,
            skuid=skuid,
            created_at=created_at,
            event_id = event_id
            )
        sd.save()

    print("===========",saveadd)
    print("=====savehome=====",savehome)
    if saveadd == 'Save & Add Other':
        return HttpResponseRedirect('/admin/events/eventslabeldetails/add/')
    if savehome == 'Save & Go To Home':
        return HttpResponseRedirect('/admin/')

def update(request,id):
    print('==== ID ==============',id)

    # Getting event id

    q1 = EventsLabelDetails.objects.get(id=id)
    eventID = q1.event_id
    labelid = q1.labelid
    skuid = q1.skuid
    dictSkuID = {'skuid':skuid}
    dictLabel = {'labelid':labelid}
    print("=========Event ID",eventID)

    # Getting Header Details

    dataForUpdate = EventsHeader.objects.filter(id=eventID).values()
    dictData=(dataForUpdate[0])

    # Getting Celebrity Name

    q2 = EventsHeader.objects.get(id=eventID)
    celebrity = q2.celebrity_name
    print(celebrity)


    celebrityMappingID = {'celebrityMappingIDforUpdate':id}
    print(celebrityMappingID)

    dictCelebrity = {'celebrityForUpdate':celebrity}

    # Getting Celebrity ID For Fetching All SKU & Label

    celebrityID = MagentoCelebProd.objects.filter(celebrity_name=celebrity).values('celebrity_id').distinct()
    celebrityID = celebrityID[0]['celebrity_id']
    print(celebrityID)

    # Getting SKU

    productID = MagentoCelebProd.objects.filter(celebrity_id=celebrityID).values('sku').distinct()
    dfProductID = pd.DataFrame(list(productID))
    if dfProductID.empty:
        listProductID=[]
    else:
        listProductID = dfProductID['sku'].values.tolist()

    # Getting Label

    labelID = MagentoCelebProd.objects.filter(celebrity_id=celebrityID).values('label').distinct()
    dfLabelID = pd.DataFrame(list(labelID))
    if dfLabelID.empty:
        listLabelID=[]
    else:
        listLabelID = dfLabelID['label'].values.tolist()

    dictProductID = {'productidForUpdate':listProductID}
    dictLabelID = {'labelidForUpdate':listLabelID}
    portalForUpdate = list(Portal.objects.values_list('portal_name',flat=True).distinct())
    dictPortal = {'portalForUpdate':portalForUpdate}
    eventForUpdate = list(EventType.objects.values_list('event_type',flat=True).distinct())
    dictEvent = {'eventForUpdate':eventForUpdate}
    eventclassForUpdate = list(EventClass.objects.values_list('event_class',flat=True).distinct())
    dictEventClass = {'eventclassForUpdate':eventclassForUpdate}
    #print(dictProductID)
    print(dictLabelID)
    context = {**dictSkuID,**dictLabel,**dictData,**dictCelebrity,**dictPortal,**dictEvent,**dictEventClass, **dictProductID,
    **dictLabelID,**celebrityMappingID}
    return render(request,'celebrity_mapping/update.html',context)


def updateSuccess(request):
    mappingID = request.POST.get("idName") 
    user = request.POST.get("user") 
    celebrity = request.POST.get("celebrityname",'')
    generic = request.POST.get("genericname",'')  
    productid = request.POST.get("productidname",'')
    labelid = request.POST.get("labelname",'') 
    eventportal = request.POST.get("eventportalname",'')
    eventtype = request.POST.get("eventtypename",'') 
    eventclass = request.POST.get("eventclassname",'')
    totalpost = request.POST.get("total_post",'') 
    bqpost = request.POST.get("bq_post",'')
    eventdate = request.POST.get("date",'')
    eventtime = request.POST.get("time",'')
    remark = request.POST.get("remark",'')

    print(type(eventdate))
    edate=datetime.datetime.strptime(eventdate, '%Y-%m-%d')
    print(type(eventtime))
    etime=datetime.datetime.strptime(eventtime, '%H:%M')
    print(type(edate))
    print(type(etime))

    print('============= values for update=====================')
    print(mappingID,user,celebrity,eventportal,eventtype,eventclass,totalpost,bqpost,eventdate, eventtime, remark)
    
    q1 = EventsLabelDetails.objects.get(id=mappingID)
    eventID = q1.event_id

    EventsHeader.objects.filter(pk=eventID).update(
        generic= generic,
        event_portal= eventportal,
        event_type= eventtype,
        event_class= eventclass,
        total_post=totalpost,
        bq_post= bqpost,
        event_date= eventdate,
        event_time= eventtime,
        remark= remark,
        updated_at=datetime.datetime.now(),
        )
    EventsLabelDetails.objects.filter(pk=mappingID).update(
        skuid= productid,
        labelid = labelid,
        updated_at=datetime.datetime.now(),
        )

    return HttpResponseRedirect('/admin/events/eventslabeldetails/')


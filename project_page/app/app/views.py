from app import app
from flask import render_template

#deal with post requests
from flask import request, redirect

#for sending requests
import requests

#for connecting to mysql database
import mysql.connector

#to handle data globally
from flask_session import Session

#to transform date occ
from datetime import datetime
import datetime as dt

# for creating spark session and aggregating data with pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as fc
from pyspark.sql.types import *

SESSION_TYPE = 'filesystem'
app.config.from_object(__name__)
Session(app)


@app.route("/")
@app.route("/index")
def index():
    return render_template("index.html")


@app.route("/summary",methods=["GET", "POST"])
def summary():
    if request.method == "POST":

        req = request.form
        place = req["area"]
        #calculate starting date
        date1 = req["start_date"] 
        ds = date1.split('/')   
        start_date = ds[2]+'-'+ds[0]+'-'+ds[1]
        s_date = datetime.strptime(date1,'%m/%d/%Y')
        s_days = (s_date - datetime(1970,1,1)).days
        #calculate ending date
        date2 = req["end_date"]
        de = date2.split('/')
        end_date = de[2]+'-'+de[0]+'-'+de[1]
        e_date = datetime.strptime(date2,'%m/%d/%Y')
        e_days = (e_date - datetime(1970,1,1)).days
        #print(e_days)
        #print(s_days)
        #print(req)
        
        """
        Developeb by Hancheng Zhu
        """
        cnx = mysql.connector.connect(user='root',password='sp50315A',host='127.0.0.1',database='arrest_data')
        cur = cnx.cursor()
        cur.execute(f"""select arrest.report_id, area.area_id, arrest.reporting_district, area_name, bureau, address, cross_street, coordinates, arrest_date, time, type, age, sex, descent  
                            from ((area join arrest 
                            on area.area_id = arrest.area_id and area.area_id = {place} and arrest_date >= '{start_date}' and arrest_date <= '{end_date}') 
                            join criminal  
                            on criminal.report_id = arrest.report_id) 
                            join address 
                            on address.report_id = arrest.report_id;""")
        global mysql_d
        mysql_d = cur.fetchall()
        cnx.close()

        global arrest_page
        arrest_page = 1

        """
        Developed by Feibai Pan
        """
        spark = SparkSession.builder.appName("arrest").getOrCreate()

        spark_data = []
        for row in mysql_d: 
            row = list(row)
            #spark dataframe 不支持datetime.timedelta格式,将时间转换成string
            row[8] = str(row[8])
            row[9] = str(row[9])
            spark_data.append(row)
        #print(spark_data[0])
        arrest_df = spark.createDataFrame(spark_data,['report_id','area_id','district','area_name','bureau','address','cross_street','coordinates', 'date','time','type','age','sex','descent'])
        arrest_df.show(5)

        #arrest_df = spark.createDataFrame([(1,2)],['report_id','area_id'])
        #print(arrest_df.collect()[0]["report_id"])
        #arrest_df.show()
        global sex_ratio
        global age
        global avg_age
        #######
        #######add this global variable
        global arrest_district
        global type_

        # count of male and female arrests
        sex = arrest_df.groupBy('sex').agg(fc.count('*').alias('num'))
        sex_ratio = sex.withColumn('ratio',fc.bround(sex.num/len(spark_data),scale=4))
        # top 10 criminal age
        age = arrest_df.groupBy('age').agg(fc.count('*').alias('num')).orderBy('num',ascending=False).limit(10)
        # criminal average age
        avg_age = arrest_df.agg(fc.avg('age').alias('avg_age'))
        # top 10 arrest district
        arst_district = arrest_df.groupBy('district').agg(fc.count('*').alias('num')).orderBy('num',ascending=False)
        # count of different types of crime
        type_ = arrest_df.groupBy('type').agg(fc.count('*').alias('num')).orderBy('num',ascending=False)

        
        #sex_ratio.show()
        #print(sex_ratio.collect()[0]['ratio'])
        sex_ratio = round(sex_ratio.collect()[1]['num']/sex_ratio.collect()[0]['num'],2)
        #age.show()
        #print(age.collect()[0]['age'])
        age = age.collect()[0]['age']
        #avg_age.show()
        #print(avg_age.collect()[0]['avg_age'])
        avg_age = round(avg_age.collect()[0]['avg_age'],2)
        #district.show()
        #print(arst_district.collect()[0]['district'])    
        arrest_district = arst_district.collect()[0]['district'] 
        #type_.show()
        #print(type_.collect()[0]['type'])
        type_ = type_.collect()[0]['type']

        """
        Developeb by Po-Han Chen
        """
        #根據request從firebase拿資料

        #define columns
        global fire_f
        fire_f = ['Date Rptd', 'DATE OCC', 'TIME OCC','AREA ', 'Rpt Dist No',  'Premis Cd', 'Weapon Used Cd', 'Status', 'Crm Cd 1', 'Crm Cd 2', 'Crm Cd 3', 'Crm Cd 4', 'LOCATION', 'Cross Street', 'LAT', 'LON','Vict Age', 'Vict Sex', 'Vict Descent'] 
        #####################
        #first method
        #find the keys
        #print(place)
        with requests.session() as s:
            reps = s.get('https://dsci551-ba051.firebaseio.com/area-index/'+place+'.json?orderBy="$value"&startAt='+str(s_days)+'&endAt='+str(e_days)+'&limitToFirst=100')
            #reps = s.get('https://dsci551-ba051.firebaseio.com/area-index/'+place+'.json?orderBy="$value"&limitToFirst=20')
            #reps = s.get('https://dsci551-ba051.firebaseio.com/area-index/'+place+'.json?orderBy="$value"&limitToFirst=20')
        print(reps.json())
        #print(s_days)
        #get all the data
        whole = dict()
        #try:
        s = requests.session()
        for key in reps.json().keys():
            rv = s.get('https://dsci551-ba051.firebaseio.com/victim.json?orderBy="$key"&equalTo="'+key+'"')
            if rv.json()[key].get('Vict Age','0') == '0':
                continue
            # elif rv.json()[key].get('Vict Sex',None) == None:
            #     continue
            rr = s.get('https://dsci551-ba051.firebaseio.com/crime-plain.json?orderBy="$key"&equalTo="'+key+'"')
            whole[key] = rr.json()[key]
            for kk in rv.json()[key].keys():
                whole[key][kk]  = rv.json()[key][kk]
        
        s.close()
        #convert whole for page rendering
        global fire_d
        fire_d = []
        #generate list of data for jinja 
        #fire_d = [data for data in [whole[key] for key in whole.keys()]
    
        for k in whole.keys():
            data = []
            # if whole[k].get('Vict Age','0') == '0':
            #     continue
            # elif whole[k].get('Vict Sex',None) == None:
            #     continue
            for f in fire_f:
                if f == 'Vict Age':
                    data.append(int(whole[k].get(f,None)))
                elif f == 'Date Rptd':
                    data.append((datetime(1970,1,1)+dt.timedelta(days=int(whole[k].get(f,None)))).strftime("%Y-%m-%d"))
                elif f == 'DATE OCC':
                    data.append((datetime(1970,1,1)+dt.timedelta(days=int(whole[k].get(f,None)))).strftime("%Y-%m-%d"))
                else:
                    data.append(whole[k].get(f,None))
            fire_d.append(data)
        # except:
        #     pass

        ####################
        #second method
        # with requests.session() as s:
        #     rr = s.get('https://dsci551-ba051.firebaseio.com/crime-plain.json?orderBy="DATE OCC"&startAt='+str(s_days)+'&endAt='+str(e_days)+'&limitToFirst=50')
        # whole = rr.json()
        # print(len(whole))
        # try:
        #     s = requests.session()
        #     for key in whole.keys():
        #         if whole[key]['AREA '] == place:
        #             rv = s.get('https://dsci551-ba051.firebaseio.com/victim.json?orderBy="$key"&equalTo="'+key+'"')
        #             for kk in rv.json()[key].keys():
        #                 whole[key][kk]  = rv.json()[key][kk]
        #     s.close()
        #     #convert whole for page rendering
        #     global fire_d
        #     fire_d = []
        #     #generate list of data for jinja 
        #     #fire_d = [data for data in [whole[key] for key in whole.keys()]
        
        #     for k in whole.keys():
        #         data = []
        #         if whole[k].get('Vict Age','0') == '0':
        #             continue
        #         elif whole[k].get('Vict Sex',None) == None:
        #             continue
        #         for f in fire_f:
        #             if f == 'Vict Age':
        #                 data.append(int(whole[k].get(f,None)))
        #             else:
        #                 data.append(whole[k].get(f,None))
        #         fire_d.append(data)
        # except:
        #     pass


        """
        Developed by Feibai Pan
        """
        schema = StructType([
            StructField("Date_Rptd", StringType(), True),
            StructField("DATE_OCC", StringType(), True),
            StructField("TIME_OCC", FloatType(), True),
            StructField("AREA", StringType(), True),
            StructField("Rpt_Dist_No", StringType(), True),
            StructField("Premis_Cd", StringType(), True),
            StructField("Weapon_Used_Cd", StringType(), True),
            StructField("Status", StringType(), True),
            StructField("Crm_Cd_1", StringType(), True),
            StructField("Crm_Cd_2", StringType(), True),
            StructField("Crm_Cd_3", StringType(), True),
            StructField("Crm_Cd_4", StringType(), True),
            StructField("LOCATION", StringType(), True),
            StructField("Cross_Street", StringType(), True),
            StructField("LAT", StringType(), True),
            StructField("LON", StringType(), True),
            StructField("Vict_Age", IntegerType(), True),
            StructField("Vict_Sex", StringType(), True),
            StructField("Vict_Descent", StringType(), True),
        ])

        crime_df = spark.createDataFrame(fire_d,schema=schema)
        #crime_df.show(5)

        global vict_sex_ratio
        global vict_age
        global avg_vict_age
        global criminal_district
        global dangerous_district

        # count of male and female victims
        vict_sex = crime_df.groupBy('Vict_sex').agg(fc.count('*').alias('num')).orderBy('Vict_sex',ascending=False)
        # top 10 victim age
        vict_age = crime_df.groupBy('Vict_age').agg(fc.count('*').alias('num')).orderBy('num',ascending=False).limit(10)
        # criminal victim age
        avg_vict_age = crime_df.agg(fc.avg('Vict_age').alias('avg_age'))
        # top crime-ridden districts
        crim_district = crime_df.groupBy('Rpt_Dist_No').agg(fc.count('*').alias('num')).orderBy('num',ascending=False)

        # estimated overall arrest rate 
        global overall_arrest_rate
        try:
            overall_arrest_rate = round(len(spark_data)/len(fire_d),2)
        except:
            overall_arrest_rate = 'error'
        # arrest rate for each district
        district_rate = crim_district.join(arst_district,crim_district.Rpt_Dist_No == arst_district.district).select(arst_district.district,crim_district.num.alias('crime'),arst_district.num.alias('arrest'))
        district_rate.show(10)
        district_rate = district_rate.withColumn('arrest_ratio',fc.bround(district_rate.arrest/district_rate.crime,scale=4)).orderBy('arrest_ratio')
        district_rate.show(20)

        
        
        #vict sex ratio
        vict_sex.show()
        vict_sex_ratio = round(vict_sex.collect()[-2]['num']/vict_sex.collect()[-1]['num'],2)
        #age.show()
        #print(vict_age.collect()[0]['Vict_age'])
        vict_age = vict_age.collect()[0]['Vict_age']
        #avg_age.show()
        #print(avg_vict_age.collect()[0]['avg_age'])
        avg_vict_age = round(avg_vict_age.collect()[0]['avg_age'],2)
        #district.show()
        #print(crim_district.collect()[0]['Rpt_Dist_No'])    
        criminal_district = crim_district.collect()[0]['Rpt_Dist_No']

        dangerous_district = district_rate.collect()[0]['district']
        #print(dangerous_district)

        #rendering pages
        return render_template("summary.html",sex_ratio=sex_ratio,age=age,avg_age=avg_age,arrest_district=arrest_district,type_=type_,vict_sex_ratio=vict_sex_ratio,vict_age=vict_age,avg_vict_age=avg_vict_age,criminal_district=criminal_district,overall_arrest_rate=overall_arrest_rate,dangerous_district=dangerous_district)
    else:
        arrest_page = 1
        return render_template("summary.html",sex_ratio=sex_ratio,age=age,avg_age=avg_age,arrest_district=arrest_district,type_=type_,vict_sex_ratio=vict_sex_ratio,vict_age=vict_age,avg_vict_age=avg_vict_age,criminal_district=criminal_district,overall_arrest_rate=overall_arrest_rate,dangerous_district=dangerous_district)
    
    
    
@app.route("/crime",methods=["GET", "POST"])
def crime():
    global crime_page
    global fire_d
    crime_page=1
    return render_template("result_crime.html",fire_d=fire_d[:10],fire_f=fire_f) 

#turning pages for crime data
@app.route("/crime_next",methods=["GET", "POST"])
def crime_next():
    global crime_page
    global fire_d
    crime_page += 1
    return render_template("result_crime.html",fire_d=fire_d[(crime_page-1)*10:crime_page*10],fire_f=fire_f) 

@app.route("/crime_prev",methods=["GET", "POST"])
def crime_prev():
    global crime_page
    global fire_d
    crime_page -= 1
    return render_template("result_crime.html",fire_d=fire_d[(crime_page-1)*10:crime_page*10],fire_f=fire_f) 


@app.route("/arrest",methods=["GET", "POST"])
def arrest():
    global arrest_page
    global mysql_d
    arrest_page=1
    return render_template("result_arrest.html",mysql_d=mysql_d[(arrest_page-1)*10:arrest_page*10]) 

#turning pages for arrest data
@app.route("/arrest_next",methods=["GET", "POST"])
def arrest_next():
    global mysql_d
    global arrest_page
    arrest_page += 1
    return render_template("result_arrest.html",mysql_d=mysql_d[(arrest_page-1)*10:arrest_page*10]) 

@app.route("/arrest_prev",methods=["GET", "POST"])
def arrest_prev():
    global mysql_d
    global arrest_page
    arrest_page -= 1
    return render_template("result_arrest.html",mysql_d=mysql_d[(arrest_page-1)*10:arrest_page*10])
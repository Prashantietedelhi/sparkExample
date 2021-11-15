import pandas as pd
import numpy as np
import csv, ast, json


from pyspark.sql.functions import udf


def name(n):
    # print(n)
    # return n
    # n1 = n["marchname"]
    n1 = n[0]
    n2=n[1]
    # n2 = n["mayname"]
    try:
        n1= json.loads(n1)
    except:
        n1 =None
    try:
        n2 = json.loads(n2)
    except:
        n2 = None
    if n1==None and n2==None:
        return False
    if n1!=None and n2==None:
        return True
    if n1==None and n2!=None:
        return True
    try:
        n1 = n1[0]
        try:
            first = n1["First"]
        except:
            first = None
        try:
            last =n1["Last"]
        except:
            last = None
    except:
        first = None
        last = None
    try:
        n2 = n2[0]
        try:
            first2 = n2["First"]
        except:
            first2 = None
        try:
            last2 =n2["Last"]
        except:
            last2 = None
    except:
        first2 = None
        last2 = None
    if first == first2 and last2==last:
        return False
    return True

def defaultval (key, dict):
    if key in dict:
        print(dict[key])
        try:
            return dict[key].lower().strip()
        except:
            return dict[key]
    return None

def employments(n):
    # n1 = n["marchemp"]
    # n2 = n["mayemp"]
    n1 = n[0]
    n2 = n[1]
    try:
        n1= json.loads(n1)
    except:
        n1 =None
    try:
        n2 = json.loads(n2)
    except:
        n2 = None
    if n1==None and n2==None:
        return False
    if n1!=None and n2==None:
        return True
    if n1==None and n2!=None:
        return True
    totalmarchemp = len(n1)
    totalmayemp = len(n2)
    if totalmarchemp !=totalmayemp:
        return True
    for n1d , n2d in zip(n1, n2):

        if defaultval("Employer",n1d)!=defaultval("Employer",n2d) or \
                defaultval("Position", n1d)!=defaultval("Position", n2d) or \
                defaultval("DateFrom",n1d)!= defaultval("DateFrom",n2d) or \
                defaultval("DateTo", n1d) != defaultval("DateTo", n2d):
            return True

    return False

def educations(n):
    # n1 = n["marchedu"]
    # n2 = n["mayedu"]
    n1 = n[0]
    n2 = n[1]
    try:
        n1= json.loads(n1)
    except:
        n1 =None
    try:
        n2 = json.loads(n2)
    except:
        n2 = None
    if n1==None and n2==None:
        return False
    if n1!=None and n2==None:
        return True
    if n1==None and n2!=None:
        return True
    totalmarchemp = len(n1)
    totalmayemp = len(n2)
    if totalmarchemp != totalmayemp:
        return True
    for n1d , n2d in zip(n1, n2):
        if len(n1d)!=len(n2d):
            return True
        if defaultval("Institution", n1d)!=defaultval("Institution", n2d) :
            return True
    return False

def keywords(n):
    # n1 = n["marchkey"]
    # n2 = n["maykey"]
    n1 = n[0]
    n2 = n[1]
    try:
        n1= json.loads(n1)
    except:
        n1 =None
    try:
        n2 = json.loads(n2)
    except:
        n2 = None
    if n1==None and n2==None:
        return False
    if n1!=None and n2==None:
        return True
    if n1==None and n2!=None:
        return True
    totalmarchemp = len(n1)
    totalmayemp = len(n2)
    if totalmarchemp != totalmayemp:
        return True

    marchk = []
    for i in n1:
        marchk.append(i['Keyword'])
    mayk = []
    for i in n2:
        mayk.append(i['Keyword'])
    marchk.sort()

    mayk.sort()
    if marchk != mayk:
        return True
    return False
def locations(n):
    # n1 = n["marchloc"]
    # n2 = n["mayloc"]
    n1 = n[0]
    n2 = n[1]
    try:
        n1= json.loads(n1)
    except:
        n1 =None
    try:
        n2 = json.loads(n2)
    except:
        n2 = None
    if n1==None and n2==None:
        return False
    if n1!=None and n2==None:
        return True
    if n1==None and n2!=None:
        return True
    try:
        marchlocCountry = n1["CurrentLocations"][0]['Country']
    except:
        marchlocCountry = None
    try:
        marchlocState = n1["CurrentLocations"][0]['State']
    except:
        marchlocState = None
    try:
        marchlocCounty = n1["CurrentLocations"][0]['County']
    except:
        marchlocCounty = None
    try:
        marchlocCity = n1["CurrentLocations"][0]['City']
    except:
        marchlocCity  = None
    try:
        marchlocZipCode = n1["CurrentLocations"][0]['ZipCode']
    except:
        marchlocZipCode = None
    try:
        maylocCountry = n2["CurrentLocations"][0]['Country']
    except:
        maylocCountry = None
    try:
        maylocState = n2["CurrentLocations"][0]['State']
    except:
        maylocState = None
    try:
        maylocCounty = n2["CurrentLocations"][0]['County']
    except:
        maylocCounty = None
    try:
        maylocCity = n2["CurrentLocations"][0]['City']
    except:
        maylocCity  = None
    try:
        maylocZipCode= n2["CurrentLocations"][0]['ZipCode']
    except:
        maylocZipCode=None

    if marchlocCountry==maylocCountry and marchlocState==maylocState and marchlocCounty==maylocCounty and marchlocCity==maylocCity\
        and marchlocZipCode==maylocZipCode:
        return False
    return True
def phones(n):
    # n1 = n["marchphones"]
    # n2 = n["mayphones"]
    n1 = n[0]
    n2 = n[1]
    try:
        n1= json.loads(n1)
    except:
        n1 =None
    try:
        n2 = json.loads(n2)
    except:
        n2 = None
    if n1==None and n2==None:
        return False
    if n1!=None and n2==None:
        return True
    if n1==None and n2!=None:
        return True

    if len(n1)!=len(n2):
        return True
    for n1d, n2d in zip(n1, n2):
        if defaultval("Number", n1d)!=defaultval("Number", n2d) and defaultval("Type", n1d)!=defaultval("Type", n2d):
            return True

    return False


def custom_prop(n):
    # n1 = n["marchcust"]
    # n2 = n["maycust"]
    n1 = n[0]
    n2 = n[1]
    if n1==n2:
        return False



    return True

def issearch(n):
    # n1 = n["marchissearch"]
    # n2 = n["mayissearch"]
    n1 = n[0]
    n2 = n[1]
    if n1==n2:
        return False



    return True

if __name__=="__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("EDGE Data Validation").config("hive.exec.dynamic.partition.mode","nonstrict").\
        config("spark.driver.memory", "15g"). \
        config("spark.driver.maxResultSize", "15g"). \
        getOrCreate()
    # spark.driver.memory=6
    limtval = 1000000
    nameData = spark.sql("select marchname, mayname from edgetest.prasinggh2570_march_may_onetoone2 limit "+str(limtval)).collect()
    empData = spark.sql("select marchemp, mayemp from edgetest.prasinggh2570_march_may_onetoone2  limit "+str(limtval)).collect()
    eduData = spark.sql("select marchedu, mayedu from edgetest.prasinggh2570_march_may_onetoone2  limit "+str(limtval)).collect()
    keyData = spark.sql("select marchkey, maykey from edgetest.prasinggh2570_march_may_onetoone2  limit "+str(limtval)).collect()
    locData = spark.sql("select marchloc, mayloc from edgetest.prasinggh2570_march_may_onetoone2  limit "+str(limtval)).collect()
    phoneData = spark.sql("select marchphones, mayphones from edgetest.prasinggh2570_march_may_onetoone2  limit "+str(limtval)).collect()
    custData = spark.sql("select marchcust, maycust from edgetest.prasinggh2570_march_may_onetoone2 limit "+str(limtval)).collect()
    isSearchData = spark.sql("select marchissearch, mayissearch from edgetest.prasinggh2570_march_may_onetoone2 limit "+str(limtval)).collect()

    nameData  = pd.DataFrame(nameData)#.values.tolist()
    empData  = pd.DataFrame(empData)#.values.tolist()
    eduData  = pd.DataFrame(eduData)#.values.tolist()
    keyData  = pd.DataFrame(keyData)#.values.tolist()
    locData  = pd.DataFrame(locData)#.values.tolist()
    phoneData  = pd.DataFrame(phoneData)#.values.tolist()
    custData  = pd.DataFrame(custData)#.values.tolist()
    isSearchData  = pd.DataFrame(isSearchData)#.values.tolist()



    ## Processing Name
    # sc = SparkContext(master="local[4]")
    # nameRdd = sc.parallelize(nameData).collect()
    # empRdd = sc.parallelize(empData).collect()
    # eduRdd = sc.parallelize(eduData).collect()
    # keyRdd = sc.parallelize(keyData).collect()
    # locRdd = sc.parallelize(locData).collect()
    # phoneRdd = sc.parallelize(phoneData).collect()
    # custRdd = sc.parallelize(custData).collect()
    # isSearchRdd = sc.parallelize(isSearchData).collect()


    # df = pd.read_csv("query-hixve-469274.csv")
    # df = df.rename(columns=lambda x: x.split(".")[-1])
    namechange = (nameData.apply(name, axis=1))
    empchange = (empData.apply(employments, axis=1))
    educhange = (eduData.apply(educations, axis=1))
    keyschange = (keyData.apply(keywords, axis=1))
    locchange = (locData.apply(locations, axis=1))
    phoneschange = (phoneData.apply(phones, axis=1))
    custpropchange = (custData.apply(custom_prop, axis=1))
    issearchchange = (isSearchData.apply(issearch, axis=1))
    totalchangewithissearch = namechange | empchange | educhange | keyschange | locchange | phoneschange | custpropchange | issearchchange
    totalchangewithoutissearch = namechange | empchange | educhange | keyschange | locchange | phoneschange | custpropchange

    import csv

    csvwriter = csv.writer(open("edge_2020_may_jobjet.csv","w"))

    namechange = dict(namechange.value_counts())
    empchange = dict(empchange.value_counts())
    educhange = dict(educhange.value_counts())
    keyschange = dict(keyschange.value_counts())
    locchange = dict(locchange.value_counts())
    phoneschange = dict(phoneschange.value_counts())
    custpropchange = dict(custpropchange.value_counts())
    issearchchange = dict(issearchchange.value_counts())
    totalchangewithissearch = dict(totalchangewithissearch.value_counts())
    totalchangewithoutissearch = dict(totalchangewithoutissearch.value_counts())

    # print(namechange)
    try:
        csvwriter.writerow(("name change True", namechange[True]) )
    except:
        csvwriter.writerow(("name change True", 0) )
    try:
        csvwriter.writerow(("name change False", namechange[False]) )
    except:
        csvwriter.writerow(("name change False", 0) )

    try:
        csvwriter.writerow(("employment change True", empchange[True]) )
    except:
        csvwriter.writerow(("employment change True", 0) )
    try:
        csvwriter.writerow(("employment change False", empchange[False]) )
    except:
        csvwriter.writerow(("employment change False", 0) )

    try:
        csvwriter.writerow(("education change True", educhange[True]) )
    except:
        csvwriter.writerow(("education change True", 0) )
    try:
        csvwriter.writerow(("education change False", educhange[False]) )
    except:
        csvwriter.writerow(("education change False", 0) )

    try:
        csvwriter.writerow(("keyword change True", keyschange[True]) )
    except:
        csvwriter.writerow(("keyword change True", 0) )
    try:
        csvwriter.writerow(("keyword change False", keyschange[False]) )
    except:
        csvwriter.writerow(("keyword change False", 0) )

    try:
        csvwriter.writerow(("location change True", locchange[True]) )
    except:
        csvwriter.writerow(("location change True", 0) )
    try:
        csvwriter.writerow(("location change False", locchange[False]) )
    except:
        csvwriter.writerow(("location change False", 0) )

    try:
        csvwriter.writerow(("phone change True", phoneschange[True]) )
    except:
        csvwriter.writerow(("phone change True", 0) )
    try:
        csvwriter.writerow(("phone change False", phoneschange[False]) )
    except:
        csvwriter.writerow(("phone change False", 0) )

    try:
        csvwriter.writerow(("custom property change True", custpropchange[True]) )
    except:
        csvwriter.writerow(("custom property change True", 0) )
    try:
        csvwriter.writerow(("custom property change False", custpropchange[False]) )
    except:
        csvwriter.writerow(("custom property change False", 0) )

    try:
        csvwriter.writerow(("is search change True", issearchchange[True]) )
    except:
        csvwriter.writerow(("is search change True", 0) )
    try:
        csvwriter.writerow(("is search change False", issearchchange[False]) )
    except:
        csvwriter.writerow(("is search change False", 0) )


    try:
        csvwriter.writerow(("total change with is search True", totalchangewithissearch[True]) )
    except:
        csvwriter.writerow(("total change with is search True", 0) )
    try:
        csvwriter.writerow(("total change with is search False", totalchangewithissearch[False]) )
    except:
        csvwriter.writerow(("total change with is search False", 0) )

    try:
        csvwriter.writerow(("total change without is search True", totalchangewithoutissearch[True]) )
    except:
        csvwriter.writerow(("total change without is search True", 0) )
    try:
        csvwriter.writerow(("total change without is search False", totalchangewithoutissearch[False]) )
    except:
        csvwriter.writerow(("total change without is search False", 0) )

    # finaldf = pd.concat(
    #     [namechange, empchange, educhange, keyschange, locchange, phoneschange, custpropchange, issearchchange], axis=1)
    # cols = namechange | empchange | educhange | keyschange | locchange | phoneschange | custpropchange | issearchchange
    # print(cols.value_counts())
    # from pyspark import SparkContext
    # import numpy as np
    # #
    # # from pyspark import SparkContext
    # # from pyspark.sql import SparkSession
    # # from pyspark.sql import SQLContext
    # #
    # # import numpy as np
    # #
    # # sc = SparkSession.builder.appName("JobFaiss").config("hive.exec.dynamic.partition.mode",
    # #                                                      "nonstrict").getOrCreate()
    # # df = spark.read.table('edgetest.prasinggh2570_march_may_onetoone2'
    # #                       )
    # # df = df.values.tolist()
    # # df = df[1:]
    # # rdd = sc.parallelize(df)
    # # rddName = rdd.map(name)
    # sc = SparkContext(master="local[4]")
    # df = df.values.tolist()
    # df = df[1:]
    # rdd = sc.parallelize(df)
    # rddName = rdd.map(name)
    # # rddEmp = rdd.map(employments)
    # # rddEdu = rdd.map(educations)
    # # rddKey = rdd.map(keywords)
    # # rddLoc = rdd.map(locations)
    # # rddPhonee = rdd.map(phones)
    # # rddCust = rdd.map(custom_prop)
    # # rddIsSearch = rdd.map(issearch)
    #
    # rddName = rddName.collect()
    # print(rddName)
    # # rddEmp = rddEmp.collect()
    # # rddEdu = rddEdu.collect()
    # # rddKey =     rddKey.collect()
    # # rddLoc = rddLoc.collect()
    # # rddPhonee = rddPhonee.collect()
    # # rddCust = rddCust.collect()
    # # rddIsSearch = rddIsSearch.collect()




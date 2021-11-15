import pandas as pd
import numpy as np
import csv, ast, json
from collections import Counter

def onetcount_part2(n):
    '''SELECT  onetcodes,activitytimestamps
                            from edge.unified_profiles_sanitized
                            '''
    try:
        lse = ast.literal_eval(n["activitytimestamps"])['Timestamps']['LatestSeenEvidence']["Timestamp"]
        sdc = ast.literal_eval(n["activitytimestamps"])['Timestamps']['SourceDeltaChanged']["Timestamp"]
        jjsa = ast.literal_eval(n["activitytimestamps"])['Timestamps']['SourceAcquired']["SourceDates"]["JobJet"]
        if  sdc == lse and jjsa == sdc :
            n1 = ast.literal_eval(n["onetcodes"])
            v = [i for i in n1 if i["Taxonomy"] == "carotenev3"]
            if len(v) > 0:
                v = v[0]
                code = v["Code"]
                try:
                    Name = v["Name"]
                except:
                    Name = ""
                return (str(Name) + " ," + str(code),)
    except Exception as e:
        print(e)
        return (None,)
    return (None,)


if __name__=="__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("EDGE Data Validation").config("hive.exec.dynamic.partition.mode","nonstrict").\
        getOrCreate()

    onetsdata = spark.sql('''SELECT  onetcodes,activitytimestamps
                            from edge.unified_profiles
                            ''')


    namechange = onetsdata.rdd.map(onetcount_part2).map(Counter).reduce(lambda x, y: x + y)
    import csv
    
    csvwriter = csv.writer(open("edge_onet_distribution_2.csv", "w"))

    for k,v in namechange.items():
        csvwriter.writerow((k,v))





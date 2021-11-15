from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import date
import json,ast
today = date.today()
classify_date = today.strftime("%Y-%m-%d")
output_table_name = "edgetest.employment_classification_carotene"
spark = SparkSession.builder.appName('JobTitle Class app').config("hive.exec.dynamic.partition.mode",
                                                                  "nonstrict").getOrCreate()
# hdfs_lib_path = "hdfs://qtmhdfs:8020/user/svcedgehadoop/lib/"
# src_dir_path = hdfs_lib_path + "src_test/"
schema = StructType([
    StructField('title', StringType()),
    StructField('description', StringType()),
    StructField('job_titles', StringType()),
    StructField('status_code', IntegerType()),
    StructField('classified_date', StringType()),
    StructField('taxonomy', StringType())
])
def parsestr(st):
    st = st.replace("title=", "title=\"")
    st = st.replace(", confidence=", "\", confidence=")
    st = st.replace("id=", "\"id\"=")
    st = st.replace("title", "\"title\"")
    st = st.replace("confidence", "\"confidence\"")
    st = st.replace("=", ":")
    st = st.replace('""', '"')
    st = st.replace("'id'", '"id"')
    st = st.replace("'confidence'", '"confidence"')
    st = st.replace("'title'", '"title"')
    st = st.replace("'\"", '"')
    st = st.replace('""', '"')
    st = st.replace(": '", ': "')
    st = st.replace("',", '",')
    st = st.replace("\"'", '"')
    st = st.replace("confidence\":nan", "confidence\":0.0")
    try:
        json.loads(st)
    except:
        raise  Exception(st)
    return json.dumps(json.loads(st))

def parse_confidence(jobtitle):
    jobtitle = json.loads(jobtitle)
    for jobti in jobtitle:
        try:
            float(jobti['confidence'])
        except:
            jobti['confidence']= 0.0
        try:
            jobti['confidence']
            jobti['title']
            jobti['id']
            jobti['id']
        except:
            raise Exception(jobtitle)
    return json.dumps(jobtitle)


def readEmployementData():
    titlesDes_df = spark.sql("SELECT * from edge.employment_classification WHERE taxonomy is not null ")
    return titlesDes_df


def process_data(data):
    employment_classification = data
    if employment_classification['taxonomy']=="carotenev3":
        job_titles = parsestr(employment_classification["job_titles"])
        # print(job_titles)
        job_titles = parse_confidence(job_titles)
        res = [employment_classification.title, employment_classification.description, job_titles,
                        employment_classification.status_code, employment_classification.classified_date,
                        employment_classification.taxonomy]
    else:
        # job_titles = parse_confidence(employment_classification["job_titles"] )
        res = [employment_classification.title, employment_classification.description,employment_classification.job_titles ,
               employment_classification.status_code, employment_classification.classified_date,
               employment_classification.taxonomy]
    return res

def process_sparkData(titlesDes_df):
    activities = titlesDes_df.rdd.map(process_data)
    activitiesDF = spark.createDataFrame(activities, schema)
    activitiesDF = activitiesDF.replace("", None)
    activitiesDF.select("title", "description", "job_titles", "status_code", "classified_date",
                        "taxonomy").createOrReplaceTempView("temp_activities")
    insert_sql = "INSERT into TABLE " + str(output_table_name) + " PARTITION( taxonomy) SELECT * FROM temp_activities"
    # insert_sql = "create TABLE " + str(output_table_name) + " as SELECT * FROM temp_activities"
    spark.sql(insert_sql)
if __name__ == '__main__':
    titlesDes_df = readEmployementData()
        process_sparkData(titlesDes_df)
    # nohup spark2-submit --num-executors 200 --executor-cores 2 --driver-cores 4 --driver-memory 8g --executor-memory 10g --conf spark.dynamicallocation.enabled=false  --conf spark.yarn.queue=edge.all --conf spark.yarn.maxappattempts=1 --conf spark.sql.shuffle.partitions=200 --conf spark.yarn.executor.memoryOverhead=10000 convert_jsons.py "AnonymousResume CareerBuilder" &
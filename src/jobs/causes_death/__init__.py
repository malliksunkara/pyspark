from operator import add
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import os
from pathlib import Path


def get_keyval(row):
    words = filter(lambda r: r is not None, row)
    return [[w.strip().lower(), 1] for w in words]


def run(spark, config):

    df = spark.read.csv(config['relative_path'] + config['deaths_file_path'], header=True, sep=",")
    print(df.dtypes)
    print(df.show())

    df.groupby('State').agg({'Deaths': 'sum'}).show()

    cause_udf = udf(lambda CauseName: "kidney disease" if CauseName.find('Nephritis') else "suicide", StringType())
    df1 = df.withColumn("Cause", cause_udf(df.CauseName))
    df1.select("Cause").show()

    df2 = df1.withColumnRenamed('Age-adjusted Death Rate', 'AgeDeathRate')
    df2.write.parquet("final.parquet")
    #print("os.path.dirname(os.path.abspath(__file__))", os.path.dirname(os.path.abspath(__file__)))
    #print("os.getcwd()" + os.getcwd())


    #path = os.getcwd + '/final.parquet'

    mypath = Path().absolute()
    print("mypath", mypath)
    filePath = mypath / 'final.parquet'
    print("filePath", filePath)

    exists = filePath.exists()

    if(exists):
        return True
    else:
        return False






from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StructField, StructType, StringType, LongType
from sys import getsizeof

# Initiating  Spark session
spark = SparkSession.builder.appName("CACI").master("local").getOrCreate()
# Defining Schema as all 3 data having same Column types we can use same schema for all.
# We can not use inferschema as in fraud data starting rows having no values

mySchema = StructType([
    StructField("Credit_Card_Number", LongType(), True),
    StructField("IP_Address", StringType(), True),
    StructField("State_Name", StringType(), True)
])
# Creating data frame for fraud Transaction
Fraud_Transactions_DF = spark \
    .read \
    .schema(mySchema) \
    .option("header", True) \
    .csv("fraud")
# Creating data frame for Transactions_01 and Transaction_02
Transactions_01_DF = spark \
    .read \
    .schema(mySchema) \
    .option("header", True) \
    .csv("transaction-001")
Transactions_02_DF = spark \
    .read \
    .schema(mySchema) \
    .option("header", True) \
    .csv("transaction-002")
# Droping Duplicates
# (In this test No duplicates checked all data through)
# Dataframename.groupBy("Credit_Card_Number",'IP_Address').count().where("count > 1").show()


Fraud_Transactions_DF.dropDuplicates(["Credit_Card_Number", "IP_Address"])
Transactions_01_DF.dropDuplicates(["Credit_Card_Number", "IP_Address"])
Transactions_02_DF.dropDuplicates(["Credit_Card_Number", "IP_Address"])

# Task 2 - Store fraud data in SQL table
# Creating Parameters for establishing connection with MYsql
jdbcHostname = "localhost"
jdbcDatabase = "CREDIT_CARD_DB"
jdbcPort = 3306
jdbcUrl = "jdbc:mysql://{0}:{1}/{2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
jdbcUsername = "root"
jdbcPassword = "Anvesh9441"
# Creating common properties
connectionProperties = {
    "user": jdbcUsername,
    "password": jdbcPassword,
    "driver": "com.mysql.jdbc.Driver"

}
# We can save both variables and connection properties in a separate method in a common library and can be used whenever required.
# Created table "CREDIT_CARD_DATA" and CREDIT_CARD_DB in Mysql and Inserting Fraud transactions data in to the table
table_Name = "CREDIT_CARD_DATA"
Fraud_Transactions_DF.write.mode('overwrite').jdbc(url=jdbcUrl, table=table_Name, properties=connectionProperties)


# Task - 3
# Method to detect credit Maker based on Number
# Whenever there is a change in Numbering for a credit card model or any new credit maker we want to add if we just need to change this method
# We can use this method in any program by placing it into a common library /Class

def credit_card_checker(n):
    n = list(str(n).strip())
    """Checks if credit card number fits the visa format."""
    if ''.join(n[:1]) in ['4']:
        return "Visa"
    elif ''.join(n[:4]) in ['5018', '5020', '5038'] or ''.join(n[:2]) in ['56']:
        return "Maestro"
    elif ''.join(n[:4]) in ['2131', '1800']:
        return "Jcb15"
    elif ''.join(n[:2]) in ['35']:
        return "Jcb16"
    elif ''.join(n[:3]) in ['300', '301', '304', '305'] or ''.join(n[:2]) in ['36', '38']:
        return "Diners"
    elif ''.join(n[:4]) in ['6011'] or ''.join(n[:2]) in ['65']:
        return "Discover"
    elif ''.join(n[:2]) in ['34', '37']:
        return "Amex"
    elif ''.join(n[:2]) in ['51', '52', '54', '55'] or ''.join(n[:3]) in ['222']:
        return "Mastercard"
    else:
        return "Wrong_Format"


# created UDF function and applied this function on both data frames on Credit_card_number column to determine type of card

udf_creditcard = f.udf(credit_card_checker, StringType())

Transactions_01_DF = Transactions_01_DF.withColumn("Credit_Card_Type", udf_creditcard("Credit_Card_Number"))
Transactions_02_DF = Transactions_02_DF.withColumn("Credit_Card_Type", udf_creditcard("Credit_Card_Number"))
# Filtered the rows which have wrong format credit card num and removed the extra column which we added to do the function

Sanitized_Transactions_01_DF = Transactions_01_DF.filter(Transactions_01_DF.Credit_Card_Type != 'Wrong_Format').drop(
    Transactions_01_DF.Credit_Card_Type)

Sanitized_Transactions_02_DF = Transactions_02_DF.filter(Transactions_02_DF.Credit_Card_Type != 'Wrong_Format').drop(
    Transactions_02_DF.Credit_Card_Type)
# Task -4
# counting no .of fraud transactions in Trnsactions datasets
(Sanitized_Transactions_02_DF.join(Fraud_Transactions_DF, ['Credit_Card_Number', 'IP_Address'])).count()
(Sanitized_Transactions_01_DF.join(Fraud_Transactions_DF, ['Credit_Card_Number', 'IP_Address'])).count()
# Preparing final and master fraud transactions where we will fill statenames from transacations 1 and transactions 2 datasets
Final_Fraud_Transactions_DF = (
    Fraud_Transactions_DF.alias('fdf').join(Sanitized_Transactions_01_DF, ['Credit_Card_Number', 'IP_Address'],
                                            'left')).join(Sanitized_Transactions_02_DF,
                                                          ['Credit_Card_Number', 'IP_Address'], 'left').select(
    'fdf.Credit_Card_Number', 'fdf.IP_Address', f.coalesce('fdf.State_Name', Sanitized_Transactions_02_DF.State_Name,
                                                           Sanitized_Transactions_01_DF.State_Name).alias('State_Name'))
# we will also add another column credit card type.
Final_Fraud_Transactions_DF = Final_Fraud_Transactions_DF.withColumn("Credit_Card_Type",
                                                                     udf_creditcard("Credit_Card_Number"))
# eventhough we will keep wrong fromat cards in data frame we will omit those rows when calculating any results
Final_Fraud_Transactions_DF.filter(Final_Fraud_Transactions_DF.Credit_Card_Type != 'Wrong_Format').groupby(
    'Credit_Card_Type').count().show()
Final_Fraud_Transactions_DF.filter(Final_Fraud_Transactions_DF.Credit_Card_Type != 'Wrong_Format').groupby(
    'State_Name').count().show()


# Masking 9 last digits
# we can reuse this function to mask any string for any num of characters
def mask(column, n):
    return f.concat(f.expr("substring(`{col}`, {n}+1, length(`{col}`)-{n})".format(col=column, n=n)), f.lit("*" * n))


n = 9
masked_Fraud_Transactions_DF = Final_Fraud_Transactions_DF.withColumn("Credit_Card_Number",
                                                                      mask(column="Credit_Card_Number", n=n))


# function to get size in bytes
def size_byte(s):
    if s == "null":
        return 0
    else:
        return getsizeof(s)


# Applying above function to get sum of colmun sizes into another column
udf_size = f.udf(size_byte, StringType())
masked_Fraud_Transactions_DF = masked_Fraud_Transactions_DF.withColumn('sum_in_bytes', sum(
    udf_size(masked_Fraud_Transactions_DF[col]) for col in masked_Fraud_Transactions_DF.columns))
#Saving file in Jason format
masked_Fraud_Transactions_DF.write.format("json").mode("overwrite").save("C:/tmp/Fraud_transactions_Json")
# I will choose Parquw binary type format to save data (Reasons mentioned in Read me )
masked_Fraud_Transactions_DF.write.format("parquet").mode("overwrite").save("C:/tmp/Fraud_transactions_Parquet")

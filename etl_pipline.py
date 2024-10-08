# git init
# git config --global user.name "Ahmad-MAA"
# git config --global user.email "musa.ahmad4abubakar@gmail.com"
# git status
# git commit -m "first commit"    
#  installing pyspark (using pip install pyspark [in the termional])
# installing psycopg2 in terminal
# install java 8
# Posgres jdbc driver(dowlaod and keepin work space directory)
#set java home
# Initialise the SparkSesion
# Create a spark dataframe
# Clean Data
#Data Transformation to 2NF

# Import necesary libraries
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter
from pyspark.sql.functions import monotonically_increasing_id
import os
import psycopg2

print('setting java8')
# Set Java home
os.environ['JAVA_HOME'] = r'C:\java8'

# Initialise the SparkSesion
spark = SparkSession.builder\
        .appName("Nuga Bank ETL")\
        .config("spark.jars", "postgresql-42.7.4.jar")\
        .getOrCreate()

# Extract the historicat data into a spark dataframe
df = spark.read.csv(r'dataset\rawdata\nuga_bank_transactions.csv', header=True, inferSchema=True)


#Data Cleaning and Transformation
#for column in df.columns:
#    print (column, 'Nulls:', df.filter(df[column].isNull()).count())


# fill up the missing values
df_clean = df.fillna({
    'Customer_Name' : 'Unknown',
    'Customer_Address' : 'Unknown',
    'Customer_City' : 'Unknown',
    'Customer_State' : 'Unknown',
    'Customer_Country' : 'Unknown',
    'Company' : 'Unknown',
    'Job_Title' : 'Unknown',
    'Email' : 'Unknown',
    'Phone_Number' : 'Unknown',
    'Credit_Card_Number' : 0,
    'IBAN' : 'Unknown',
    'Currency_Code' : 'Unknown',
    'Random_Number' :0.0,
    'Category' : 'Unknown',
    'Group' : 'Unknown',
    'Is_Active' : 'Unknown',
    'Description' : 'Unknown',
    'Gender': 'Unknown',
    'Marital_Status' : 'Unknown'
})

# drop the missing Values in the Last_Updated column
df_clean = df_clean.na.drop(subset = ['Last_Updated'])

#Data Cleaning and Transformation
#for column in df_clean.columns:
#    print (column, 'Nulls:', df_clean.filter(df_clean[column].isNull()).count())


#Data Transformation to 2NF

# Transaction table
transaction = df_clean.select('Transaction_Date','Amount','Transaction_Type')\
                                .withColumn('transaction_id', monotonically_increasing_id())\
                                .select('transaction_id','Transaction_Date','Amount','Transaction_Type')


# Customer table
customer = df_clean.select( 'Customer_Name','Customer_Address','Customer_City',\
                                     'Customer_State','Customer_Country','Email','Phone_Number').distinct()\
                    .withColumn('customer_id', monotonically_increasing_id())\
                    .select('customer_id','Customer_Name','Customer_Address','Customer_City',\
                                     'Customer_State','Customer_Country','Email','Phone_Number')


# employee table
employee = df_clean.select('Company','Job_Title','Gender','Marital_Status').distinct()\
                    .withColumn('employee_id', monotonically_increasing_id())\
                    .select('employee_id','Company','Job_Title','Gender','Marital_Status')


# Building the Fact_table
fact_table = df_clean.join(customer,['Customer_Name','Customer_Address','Customer_City',\
                                     'Customer_State','Customer_Country','Email','Phone_Number'], 'inner')\
                    .join(transaction,['Transaction_Date','Amount','Transaction_Type'],'inner')\
                    .join(employee,['Company','Job_Title','Gender','Marital_Status'],'inner')\
                    .select('transaction_id','customer_id','employee_id','Credit_Card_Number',\
                            'IBAN','Currency_Code','Random_Number','Category','Group', 'Is_Active',\
                             'Last_Updated', 'Description')


#Databloading
def get_db_connection():
    connection = psycopg2.connect(
        host ='localhost',
        database ='nuga_bank',
        user ='postgres',
        password ='password'
    )
    return connection
#connect to sql database
conn = get_db_connection()


def create_tables():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        create_table_query = '''
                            DROP TABLE IF EXISTS customer;
                            DROP TABLE IF EXISTS transaction;
                            DROP TABLE IF EXISTS employee;
                            DROP TABLE IF EXISTS fact_table;

                            CREATE TABLE customer (
                                customer_id BIGINT,
                                Customer_Name VARCHAR(1000),
                                Customer_Address VARCHAR(1000),
                                Customer_City VARCHAR(1000),
                                Customer_State VARCHAR(1000),
                                Customer_Country VARCHAR(1000),
                                Email VARCHAR(1000),
                                Phone_Number VARCHAR(1000)                    
                            );

                            CREATE TABLE transaction (
                                transaction_id BIGINT,
                                Amount FLOAT,
                                Transaction_Date DATE,
                                Transaction_Type VARCHAR(1000)
                            );

                            CREATE TABLE employee(
                                employee_id BIGINT,
                                Company VARCHAR(1000),
                                Job_Title VARCHAR(1000),
                                Gender VARCHAR(1000),
                                Marital_Status VARCHAR(1000)
                            );

                            CREATE TABLE fact_table(
                                transaction_id BIGINT,
                                customer_id BIGINT,
                                employee_id BIGINT,
                                Credit_Card_Number VARCHAR(1000),
                                IBAN VARCHAR(1000),
                                Currency_Code VARCHAR(1000),
                                Random_Number FLOAT,
                                Category VARCHAR(1000),
                                "Group" VARCHAR(1000),
                                Is_Active VARCHAR(1000),
                                Last_Updated DATE,
                                Description VARCHAR(1000)
                            );
                            '''
        cursor.execute(create_table_query)
        conn.commit()
        print("Tables created successfully.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

create_tables()

url = "jdbc:postgresql://localhost:5432/nuga_bank"
properties = {
    "user" : "postgres",
    "password" : "password",
    "driver" : "org.postgresql.Driver"
}

# Loading to data base
customer.write.jdbc(url=url, table="customer", mode="append", properties=properties)
employee.write.jdbc(url=url, table="employee", mode="append", properties=properties)
transaction.write.jdbc(url=url, table="transaction", mode="append", properties=properties)
fact_table.write.jdbc(url=url, table="fact_table", mode="append", properties=properties)

print('database, table and data uploaded successfully')

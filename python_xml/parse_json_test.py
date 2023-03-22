from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType
from pyspark.sql.functions import *
import ast

PURCHASE_JSON_FILE = 'resources/data/json/purchase_orders.json'
CONNECTION_PROPERTIES = {  
        "url": f"jdbc:postgresql://localhost:5432/parse_xml",
        "driver": "org.postgresql.Driver",
        "user": 'postgres',
        "password": 'postgres'
    }

TABLE='test_parse_example'

@udf(returnType=StringType())
def parse_items(item):
    item = ast.literal_eval(item)
    
    if isinstance(item,dict):
        return f"{item['PartNumber']}:{item['Quantity']}:{item['USPrice']}"
    else:
        item_array = item
        return '|'.join([f"{item['PartNumber']}:{item['Quantity']}:{item['USPrice']}" for item in item_array])

def write_to_db(df: DataFrame):
    (
        df.write
            .format("jdbc")
            .option("url", CONNECTION_PROPERTIES["url"])
            .option("dbtable", TABLE)
            .option("user", CONNECTION_PROPERTIES['user'])
            .option("password", CONNECTION_PROPERTIES['password'])
            .mode('overwrite')
            .option("driver", CONNECTION_PROPERTIES["driver"])
            .save()
    )

if __name__ == '__main__':

    spark:SparkSession = (SparkSession
                          .builder
                          .config('spark.jars.packages','org.postgresql:postgresql:42.5.4')
                          .getOrCreate() 
                          )
    

    df_pruchase_orders = spark.read.json(PURCHASE_JSON_FILE,multiLine=True)

    df_purchases = df_pruchase_orders.selectExpr('explode(PurchaseOrders) as purchases')

    df_purchases_address = df_purchases.selectExpr(
                                            'purchases.PurchaseOrderNo as po',
                                            'purchases.DeliveryNotes',
                                            'explode(purchases.Address) Address',
                                            'purchases.Items')


    df_purchases_address_Billing = df_purchases_address.where("Address.Type = 'Billing'")


    df_purchases_itens = df_purchases_address_Billing.selectExpr(
                                                        'po',
                                                        'Address.Type as billing_name',
                                                        'Address.Street as billing_street',
                                                        'Address.City as billing_city',
                                                        'Address.State as billing_state',
                                                        'Address.Zip as billing_zip',
                                                        'Address.Country as billing_country',
                                                        'Items'
                                                    )

    df_purchases_itens_concatenated = df_purchases_itens.withColumn('Items', parse_items('Items.Item') )

    df_purchases_itens_concatenated.show()


    write_to_db(df_purchases_itens_concatenated)


    spark.stop()

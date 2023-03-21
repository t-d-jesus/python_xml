from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.types import *

if __name__ == '__main__':

    spark:SparkSession = (SparkSession
                          .builder
                          .config('spark.jars.packages','com.databricks:spark-xml_2.12:0.16.0,org.postgresql:postgresql:42.5.4')
                          .getOrCreate() )


    customSchema = StructType([
                        StructField("name", StringType(), True),
                        StructField("fictional:character", StringType(), True),
                        StructField("job_name", StringType(), True),
                        StructField("job_salary", DecimalType(6,2), True)
                        ])

    df_actors:DataFrame = (
        spark.read
        .format("xml")
        .options(rowTag='actor', rootTag='actors')
        .load("resources/data/xml/actors.xml",schema = customSchema)
    )

    df_actors.show(truncate=False)
    df_actors.printSchema()

    connection_properties = {  
                "url": f"jdbc:postgresql://localhost:5432/parse_xml",
                "driver": "org.postgresql.Driver",
            }


    (
        df_actors.write.format("jdbc")
        .option("url", connection_properties["url"])
        .option("dbtable", 'actor_xml_spark')
        .option("user", 'postgres')
        .option("password", 'postgres')
        .mode('overwrite')
        .option("driver", connection_properties["driver"])
        .save()
    )

    (
      spark.read.format("jdbc")
        .option("url", connection_properties["url"])
        .option("dbtable", 'actor_xml_spark')
        .option("user", 'postgres')
        .option("password", 'postgres')
        .option("driver", connection_properties["driver"])
        .option("isolationLevel", "READ_UNCOMMITTED")
        .load()
        .show()
    )


    spark.stop()

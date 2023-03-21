from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.types import *

if __name__ == '__main__':

    spark:SparkSession = (SparkSession
                          .builder
                          .config('spark.jars.packages','org.postgresql:postgresql:42.5.4')
                          .getOrCreate() )



    JSON_FILE="resources/data/json/actors.json"

    JSON_SCHEMA = spark.read.json(JSON_FILE,multiLine=True).schema

    df_actors_json = (
        spark.read.json(JSON_FILE,multiLine=True,schema=JSON_SCHEMA)
        .selectExpr('explode(actors) as actor')
        .selectExpr('actor.gdppc','actor.neighbor.direction','actor.neighbor.name','actor.rank','actor.year')
        )
    
    df_actors_json.printSchema()
    df_actors_json.show(truncate=False)


    connection_properties = {  
                "url": f"jdbc:postgresql://localhost:5432/parse_xml",
                "driver": "org.postgresql.Driver",
            }


    (
        df_actors_json.write.format("jdbc")
        .option("url", connection_properties["url"])
        .option("dbtable", 'actor_json_spark')
        .option("user", 'postgres')
        .option("password", 'postgres')
        .mode('overwrite')
        .option("driver", connection_properties["driver"])
        .save()
    )

    (
      spark.read.format("jdbc")
        .option("url", connection_properties["url"])
        .option("dbtable", 'actor_json_spark')
        .option("user", 'postgres')
        .option("password", 'postgres')
        .option("driver", connection_properties["driver"])
        .option("isolationLevel", "READ_UNCOMMITTED")
        .load()
        .show()
    )


    spark.stop()

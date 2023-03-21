import psycopg2
 
def get_connection(host,port,database,user,password):
    return psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )


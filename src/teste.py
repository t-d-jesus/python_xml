from db_connection.postgresqldb import get_connection
from model.ActorModel import Actor
from xml_parser.actor_parser import actor_parser

if __name__ == "__main__":

    xml_file = 'resources/data/xml/xml1.xml'
    actor_list:list[Actor] = actor_parser(xml_file,'./country')

    values = list[tuple]()
    for actor in actor_list:
        print(actor)
        values.append( (actor.rank,actor.year,actor.gdppc, str(actor.neighbor) ) )


    conn = get_connection(
        database="parse_xml",
        user='postgres',
        password='postgres',
        host='127.0.0.1',
        port='5432'
        )

    conn.autocommit = True

    cursor = conn.cursor()

    cursor.executemany("INSERT INTO actor VALUES(%s,%s,%s,%s)", values)

    sql1 = '''select * from actor;'''

    cursor.execute(sql1)    


for i in cursor.fetchall():
    print(i)
 
conn.commit()
 
conn.close()
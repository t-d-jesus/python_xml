import json
import pprint
from model.ActorModel import Actor
from db_connection.postgresqldb import get_connection


if __name__ == '__main__':
    JSON_FILE = 'resources/data/json/actors.json'

    with open(JSON_FILE) as jf:
       json_file = json.load(jf)
    
 
    actor_list:list[Actor]=list()

    for actor_json in json_file['actors']:
        actor = Actor(
                            rank=actor_json["rank"],
                            year=actor_json["year"],
                            gdppc=actor_json["gdppc"],
                            neighbor=actor_json["neighbor"]
                            # neighbor=child.find("neighbor").attrib.get('name')
                            )
        actor_list.append(actor)

    values = list[tuple]()
    for actor in actor_list:
        # print(actor)
        values.append( (actor.rank,actor.year,actor.gdppc, str(actor.neighbor) ) )


    print( values )

    conn = get_connection(
        database="parse_xml",
        user='postgres',
        password='postgres',
        host='127.0.0.1',
        port='5432'
        )


    cursor = conn.cursor()

    cursor.executemany("INSERT INTO actor VALUES(%s,%s,%s,%s)", values)

    sql1 = '''select * from actor;'''

    cursor.execute(sql1)    


    for actor in cursor.fetchall():
        print(actor)
    
    conn.commit()
    
    conn.close()
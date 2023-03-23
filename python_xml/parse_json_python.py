import json
import pprint
from model.ActorModel import Actor
from db_connection.postgresqldb import get_connection
from collections import namedtuple


if __name__ == '__main__':
    PURCHASE_JSON_FILE = 'resources/data/json/purchase_orders.json'

    with open(PURCHASE_JSON_FILE) as jf:
       purchase_orders = json.load(jf)
    
    values = list[tuple]()

    purchases_tuple = namedtuple('purchases','po, billing_name, billing_street, billing_city, billing_state, billing_zip, billing_country, items')
    purchase_Orders_parsed = []
    for purchase_order in purchase_orders['PurchaseOrders']:
       address = [item for item in purchase_order['Address'] if item['Type']=='Billing'][0]

       purchase_items = None
       
       purchase_items = purchase_order['Items']
       purchase_items_string=None

       for key,value in purchase_items.items():
          if isinstance(value,list) :
            purchase_items_string = '|'.join([f"{item['PartNumber']}:{item['Quantity']}:{item['USPrice']}" for item in value])
          else:
             purchase_items=purchase_items['Item']
             purchase_items_string = f"{purchase_items['PartNumber']}:{purchase_items['Quantity']}:{purchase_items['USPrice']}"


       purchase_Orders_parsed.append(
            purchases_tuple(
                po=purchase_order['PurchaseOrderNo'],
                billing_name=address['Type'],
                billing_street=address['Street'],
                billing_city=address['City'],
                billing_state=address['State'],
                billing_zip=address['Zip'],
                billing_country=address['Country'],
                items=purchase_items_string
            )
          ) 
       

    conn = get_connection(
        database="parse_xml",
        user='postgres',
        password='postgres',
        host='127.0.0.1',
        port='5432'
        )


    cursor = conn.cursor()

    cursor.executemany("INSERT into purchases_python VALUES(%s,%s,%s,%s,%s,%s,%s,%s)", purchase_Orders_parsed)

    sql1 = '''select * from purchases_python;'''

    cursor.execute(sql1)    


    for actor in cursor.fetchall():
        print(actor)
    
    conn.commit()
    
    conn.close()
    
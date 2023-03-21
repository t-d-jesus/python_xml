import xml.etree.ElementTree as ET
from model.ActorModel import Actor

def actor_parser(xml_file,root_xpath) -> list[Actor]:
    tree = ET.parse(xml_file)
    root = tree.getroot()

    actor_list = list()

    for child in root.findall(root_xpath):
        actor = Actor(
                            rank=child.find("rank").text,
                            year=child.find("year").text,
                            gdppc=child.find("gdppc").text,
                            neighbor=child.find("neighbor").attrib
                            # neighbor=child.find("neighbor").attrib.get('name')
                            )
        actor_list.append(actor)

    return actor_list

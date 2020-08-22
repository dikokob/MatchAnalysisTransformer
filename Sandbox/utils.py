"""
    X Query Utils
        - Parser and Conditional Queries against XML

        How to read an xml file and getroot() before loopiing or findall parsing: https://docs.python.org/3/library/xml.etree.elementtree.html
        How to query a xml node on condition: https://stackoverflow.com/questions/1457638/get-nodes-where-child-node-contains-an-attribute
"""


import xml.etree.ElementTree as ET

if __name__ == '__main__':

    tree = ET.parse('data/raw_data/g1059702/f24-8-2019-1059702-eventdetails.xml')
    root = tree.getroot()

    # for child in root.findall("Game"):
    #     print("\n\nGet it at game node level \n",child.tag, child.attrib)
    #     break

    # for child in root.findall("Game/Event/"):
    #     print("\n\nGet it at event node level \n", child.tag, child.attrib)
    #     break

    # for child in root.findall("Game/Event/[@event_id = '838']"):
    #     print("\n\nGet all events on a condition \n",child.tag, child.attrib)

    for event in root.findall("Game/Event/[@type_id = '2']/Q[@qualifier_id = '5']"):
        print("\n\nGet all events on a condition \n",event.tag, event.attrib)
        # for inner_event in event.findall("Event/[@type_id = '5']"):
        #     print("Inner\n", inner_event.tag)
            # print("\n\nGet all events on a condition \n",inner_event.tag, inner_event.attrib)
        break


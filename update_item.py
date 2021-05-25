#!/usr/bin/python3
# -*- coding: utf-8 -*-
#Modules externes
import os
import re
import logging
import csv
import json
import sys
import requests
import xml.etree.ElementTree as ET
from chardet import detect

#Modules maison
from Abes_Apis_Interface.AbesXml import AbesXml
from Alma_Apis_Interface import Alma_Apis_Records
from Alma_Apis_Interface import Alma_Apis
from logs import logs

SERVICE = "Modif_exemplaires"

LOGS_LEVEL = 'INFO'
LOGS_DIR = os.getenv('LOGS_PATH')


REGION = 'EU'
INSTITUTION = 'ub'
API_KEY = os.getenv('TEST_UB_API')

FILE_NAME = 'test'
IN_FILE = '/media/sf_Partage_LouxBox/{}.csv'.format(FILE_NAME)
OUT_FILE = '/media/sf_Partage_LouxBox/{}_Rapport.csv'.format(FILE_NAME)
ITEM_XSD = 'https://developers.exlibrisgroup.com/wp-content/uploads/alma/xsd/rest_item.xsd'

xsd = {'xs': 'http://www.w3.org/2001/XMLSchema'}

# ERROR_FILE = '/media/sf_Partage_LouxBox/{}_Erreurs.csv'.format(FILE_NAME)

# get file encoding type
def get_encoding_type(file):
    with open(file, 'rb') as f:
        rawdata = f.read()
    return detect(rawdata)['encoding']

# test headers : les noms des colonnes doivent correspondre à des noms de champs d'exemplaires dans Alma
def test_headers(headers) :
    r = requests.get(ITEM_XSD)
    try:
        r.raise_for_status()  
    except requests.exceptions.HTTPError:
        raise HTTPError(r,self.service)
    reponse = r.content.decode('utf-8')
    reponsexml = ET.fromstring(reponse)
    item_data = reponsexml.find("xs:complexType[@name='item_data']/xs:all",xsd)
    # print(ET.tostring(item_data))
    for field in headers:
        if item_data.find("xs:element[@name='{}']".format(field),xsd):
            if (item_data.find("xs:element[@name='{}']/xs:annotation/xs:appinfo/xs:tags".format(field),xsd).text == 'api get post put') :
                log_module.info("Le champ {} peut bien être modifié par API".format(field))
            else :
                return (0, "Erreur nommage colonne : {} n'est pas un chanps autorisé à l'écriture dans Alma".format(field))
            # return reponsexml.find("sru:numberOfRecords",ns).text
        else : 
            return (0, "Erreur nommage colonne : {} n'est pas un chanps exemplaire connu dans Alma".format(field))
    return(1,"Test du nomage des champs terminé")

#Init logger
logs.init_logs(LOGS_DIR,SERVICE,LOGS_LEVEL)
log_module = logging.getLogger(SERVICE)


alma_api = Alma_Apis_Records.AlmaRecords(apikey=API_KEY, region=REGION, service=SERVICE)

report = open(OUT_FILE, "w",  encoding='utf-8')
report.write("Code-barres\tStatut\tMessage\n")


###Update item sequence
# ###################### 
from_codec = get_encoding_type(IN_FILE)
with open(IN_FILE, 'r', encoding=from_codec, newline='') as f:
    reader = csv.reader(f, delimiter=';')
    headers = next(reader)
    print(headers)
    del headers[0]
    code_erreur, message = test_headers(headers)
    if code_erreur == 0 :
        log_module.error(message)
        sys.exit()
    log_module.info(message)
    # We read the file
    # for row in reader:
    #     if len(row) < 2:
    #         continue
    #     barcode = row[0]
    #     # print(barcode)
    #     status,item = alma_api.get_item_with_barcode(barcode, accept='json')
    #     if status == "Error" :
    #         report.write("{}\t{}\t{}\n".format(barcode,status,item))
    #         continue
    #     i = 1
    #     bib_id = item["bib_data"]["mms_id"]
    #     holding_id = item["holding_data"]["holding_id"]
    #     item_id = item["item_data"]["pid"]
    #     for field in headers:
    #         item["item_data"][field] = row[i]
    #         i += 1
    #     status,reponse = alma_api.set_item(bib_id, holding_id, item_id, json.dumps(item), content_type='json', accept='json')
    #     if status == "Error" :
    #         report.write("{}\t{}\t{}\n").format(barcode,status,reponse)
    #     else :
    #         report.write("{}\tSuccès\tItem mis à jour\n".format(barcode,status,reponse))
        
    #     log_module.info(barcode)
report.close

log_module.info("FIN DU TRAITEMENT")

                    
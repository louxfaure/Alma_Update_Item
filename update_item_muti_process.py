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
import time
import xml.etree.ElementTree as ET
from chardet import detect
import multiprocessing

#Modules maison
from Abes_Apis_Interface.AbesXml import AbesXml
from Alma_Apis_Interface import Alma_Apis_Records
from Alma_Apis_Interface import Alma_Apis
from logs import logs


SERVICE = "test_threading"

LOGS_LEVEL = 'INFO'
LOGS_DIR = os.getenv('LOGS_PATH')


REGION = 'EU'
INSTITUTION = 'ub'
API_KEY = os.getenv('TEST_UB_API')

FILE_NAME = 'test_threading'
IN_FILE = '/media/sf_Partage_LouxBox/{}.csv'.format(FILE_NAME)
OUT_FILE = '/media/sf_Partage_LouxBox/{}_Rapport.csv'.format(FILE_NAME)
ITEM_XSD = 'https://developers.exlibrisgroup.com/wp-content/uploads/alma/xsd/rest_item.xsd'

xsd = {'xs': 'http://www.w3.org/2001/XMLSchema'}

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
        raise HTTPError(r,SERVICE)
    reponse = r.content.decode('utf-8')
    reponsexml = ET.fromstring(reponse)
    item_data = reponsexml.find("xs:complexType[@name='item_data']/xs:all",xsd)
    # print(ET.tostring(item_data))
    for field in headers:
        if item_data.find("xs:element[@name='{}']".format(field),xsd):
            if (item_data.find("xs:element[@name='{}']/xs:annotation/xs:appinfo/xs:tags".format(field),xsd).text == 'api get post put') :
                log_module.info("Le champ {} peut bien être modifié par API".format(field))
            else :
                return (0, "Erreur nommage colonne : {} n'est pas un champ autorisé à l'écriture dans Alma".format(field))
            # return reponsexml.find("sru:numberOfRecords",ns).text
        else : 
            return (0, "Erreur nommage colonne : Le champ {} n'est pas un champ exemplaire connu dans Alma".format(field))
    return(1,"Test du nomage des champs terminé")

#Init logger
logs.init_logs(LOGS_DIR,SERVICE,LOGS_LEVEL)
log_module = logging.getLogger(SERVICE)


alma_api = Alma_Apis_Records.AlmaRecords(apikey=API_KEY, region=REGION, service=SERVICE)


def init(queue):
    global idx
    idx = queue.get()

def thread(x):
    global idx
    headers = x[0]
    barcode = x[1]
    process = multiprocessing.current_process()
    status,item = alma_api.get_item_with_barcode(barcode, accept='json')
    # log_module.info("{}:{}:{}:{}:{}".format(x[1],x[2],x[3],x[4],x[5]))
    if status == "Error" :
        log_module.error("{}:{}:{}:{}".format(idx,process.pid,barcode,item))
        return barcode,"Erreur",item
    else :
        # log_module.info("{}:{}:{}:{}".format(idx,process.pid,barcode,item["item_data"]["barcode"]))
        i = 2
        bib_id = item["bib_data"]["mms_id"]
        holding_id = item["holding_data"]["holding_id"]
        item_id = item["item_data"]["pid"]
        for field in headers:
            item["item_data"][field] = x[i]
            i += 1
        status,reponse = alma_api.set_item(bib_id, holding_id, item_id, json.dumps(item), content_type='json', accept='json')
        if status == "Error" :
            log_module.error("{}:{}:{}".format(os.getpid(),barcode,reponse))
            return barcode,"Erreur",reponse
        else :
            log_module.info("{}:{}:{}:{}".format(os.getpid(),barcode,item["item_data"]["barcode"],reponse["item_data"]["barcode"]))
            return barcode, "Succés", "Exemplaire mis à jour"


        
###Update item sequence
# ###################### 
start_time = time.time()
from_codec = get_encoding_type(IN_FILE)

# Traiteemnt des données en entrée
with open(IN_FILE, 'r', encoding=from_codec, newline='') as f:
    reader = csv.reader(f, delimiter='\t')
    # On s'assure que les noms de colonne sont bons est que les champs proposés sont bien éditable via API
    headers = next(reader)
    del headers[0]
    rows=[]
    code_erreur, message = test_headers(headers)
    if code_erreur == 0 :
        log_module.error(message)
        sys.exit()
    log_module.info(message)
    num = 0
    # On stocke les données
    for row in reader:
        if len(row) < 2:
            continue
        row.insert(0, headers)
        rows.append(row)

# Prépartion et exécution des processus multimples
if __name__ == '__main__':
    ids = [0, 1, 2, 3,4,5]
    manager = multiprocessing.Manager()
    idQueue = manager.Queue()
    for i in ids:
        idQueue.put(i)
    p = multiprocessing.Pool(8, init, (idQueue,))
    with open(OUT_FILE, "w",  encoding='utf-8') as f:
        f.write("Code-barres\tStatut\tMessage\n")
        for result in p.imap(thread, rows):
            f.write("{}\t{}\t{}\n".format(*result))
    log_module.info("FIN DU TRAITEMENT :: {}".format(time.time() - start_time))





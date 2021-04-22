#!/usr/bin/python3
# -*- coding: utf-8 -*-
#Modules externes
import os
import re
import logging
import csv
import json
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
API_KEY = os.getenv('PROD_UB_BIB_API')

FILE_NAME = 'test'
IN_FILE = '/media/sf_Partage_LouxBox/{}.csv'.format(FILE_NAME)
OUT_FILE = '/media/sf_Partage_LouxBox/{}_Rapport.csv'.format(FILE_NAME)
# ERROR_FILE = '/media/sf_Partage_LouxBox/{}_Erreurs.csv'.format(FILE_NAME)

# get file encoding type
def get_encoding_type(file):
    with open(file, 'rb') as f:
        rawdata = f.read()
    return detect(rawdata)['encoding']

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
    # We read the file
    for row in reader:
        if len(row) < 2:
            continue
        barcode = row[0]
        # print(barcode)
        status,item = alma_api.get_item_with_barcode(barcode, accept='json')
        if status == "Error" :
            report.write("{}\t{}\t{}\n").format(barcode,status,item)
            continue
        i = 1
        bib_id = item["bib_data"]["mms_id"]
        holding_id = item["holding_data"]["holding_id"]
        item_id = item["item_data"]["pid"]
        for field in headers:
            item["item_data"][field] = row[i]
            i += 1
        status,reponse = alma_api.set_item(bib_id, holding_id, item_id, json.dumps(item), content_type='json', accept='json')
        if status == "Error" :
            report.write("{}\t{}\t{}\n").format(barcode,status,reponse)
        else :
            report.write("{}\tSuccès\tItem mis à jour\n".format(barcode,status,reponse))
        
        log_module.info(barcode)
report.close

log_module.info("FIN DU TRAITEMENT")

                    
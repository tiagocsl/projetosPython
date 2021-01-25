import json
import requests
from datetime import datetime, timedelta
import dateutil.parser as dp
from collections import Counter
import time

ABB_KEY = ""
ABB_URL = "https://api.smartsensor.abb.com/"
ABB_TOKEN_URL = "Auth/Key"
ABB_ORGANIZATION_URL = "Organization"
ABB_ASSET_URL = "Asset/List"
ABB_EVENT_URL = "EventLog"

UBIDOTS_ENDPOINT = "http://industrial.api.ubidots.com/api/v1.6/devices/"
UBIDOTS_TOKEN = ""

TokenAtual = None
OrganizationsList = list()
devices = list()
variables = [2,4,8,9,10,15,27,31,32,33,64]

linksArrays_ubidots = list()
linksArrays_ABB = list()

last_timestamps_list_ubidots = list()
last_timestamps_list_ABB = list()
init_days_list = list()
final_days_list = list()

devices_Lables_ubidots = list([])
devices_ids_ABB = list([])
devices_to_update = list([])
devicesID_to_update = list([])

header_ubidots = {"X-Auth-Token": UBIDOTS_TOKEN, "Content-Type": "application/json"}
header = {"Authorization": TokenAtual, "FeatureCode": "EXT_AssetTrendData"}
 

##### FUNÇÃO PARA CRIAÇÃO DO TOKEN ABB ######
def generate_Token():
	
	global TokenAtual

	parameterKey = {
	  "apiKey": ABB_KEY,
	  "deviceUID": "string"
	}

	requestToken = requests.post(ABB_URL + ABB_TOKEN_URL,json=parameterKey)

	if (requestToken.status_code) == 200:
	  print("Token gerado com sucesso!")
	  
	else:
	  print("Erro {} ao gerar o Token!".format(requestToken.status_code))

	TokenAtual = "Bearer " + requestToken.json()["authToken"]
	
	
	return(TokenAtual)

##### FUNÇÃO PARA LEITURA DAS ORGANIZAÇÕES ######
def organization_list(TokenAtual):

	global OrganizationID
	global OrganizationName

	header = {
	  "Authorization": TokenAtual,
	  "FeatureCode": "EXT_AssetTrendData"
	  
	}

	requestOrganization = requests.get(ABB_URL + ABB_ORGANIZATION_URL, headers=header)

	if (requestOrganization.status_code) == 200:
	  print("Organização requisitada com sucesso!")
	else:
	  print("Erro {} ao requisitar organização!".format(requestOrganization.status_code))
	
	for i in requestOrganization.json():
	  OrganizationName = i['organizationName']
	  OrganizationID = i['organizationID']

	return(OrganizationID)

def getSyncs(TokenAtual):
######################### PEGANDO O LASTSYNC's DA UBIDOTS #########################
	i=-1
	j=len(devices_Lables_ubidots)-1
	while (i < j):
	    i += 1
	    linksArrays_ubidots.append("https://industrial.api.ubidots.com/api/v1.6/devices/"+devices_Lables_ubidots[i]+"/potencia_saida/values?page_size=1")
    
	v = -1
	vrange = len(devices_Lables_ubidots)-1
	while (v < vrange):
	    v += 1
	    requestAsset = requests.get(linksArrays_ubidots[v], headers=header_ubidots)
	    reponseResult = requestAsset.json()

	    for lastTS in reponseResult["results"]:
	        lastTstamp = lastTS["timestamp"]

	        timestamp, ms = divmod(lastTstamp, 1000)
	        dt = datetime.fromtimestamp(timestamp) + timedelta(milliseconds=ms)
	        lastTimestamp = str(dp.parse(dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-13]) + timedelta(hours=1))
	        last_timestamps_list_ubidots.append(lastTimestamp)

	o = json.dumps(last_timestamps_list_ubidots, indent=4)
	      
#########################    PEGANDO O LASTSYNC DA ABB    #########################
	header = {"Authorization": TokenAtual, "FeatureCode": "EXT_AssetTrendData"}
	i2=-1
	j2=len(devices_ids_ABB)-1
	while (i2 < j2):
	    i2 += 1
	    linksArrays_ABB.append("https://api.smartsensor.abb.com/asset/"+devices_ids_ABB[i2])

	v2 = -1    
	vrange2 = len(devices_ids_ABB)-1
	while (v2 < vrange2):
	    v2 += 1
	    requestAsset = requests.get(linksArrays_ABB[v2], headers=header)
	    reponseResult = requestAsset.json()        

	    lastTstamp = str(dp.parse(reponseResult["lastSyncTimeStamp"]) - timedelta(hours=2))[:-12] + ":00:00"
	    last_timestamps_list_ABB.append(lastTstamp)

	p = json.dumps(last_timestamps_list_ABB, indent=4)
	#print("UBI SYNCS")
	#print(o)
	#print("ABB SYNCS")
	#print(p)

######################### COMPARANDO SYNCS DA UBIDOTS COM DA ABB #########################
def comparator():
	i = -1
	j = len(devices_ids_ABB)-1

	
	while (i < j):
		i += 1
		if (last_timestamps_list_ubidots[i] < last_timestamps_list_ABB[i]):
			init_days_list.append(last_timestamps_list_ubidots[i])
			final_days_list.append(last_timestamps_list_ABB[i])
			devices_to_update.append(devices_Lables_ubidots[i])
			devicesID_to_update.append(devices_ids_ABB[i])


	q = json.dumps(init_days_list, indent=4)
	r = json.dumps(final_days_list, indent=4)
	s = json.dumps(devices_to_update, indent=4)
	print("INIT LIST")
	print(q)
	print("FINAL LIST")
	print(r)
	print("DISPOSITIVOS A SEREM ATUALIZADOS")
	print(s)

#########################  FUNÇÃO PARA LEITURA DOS DISPOSITIVOS  #########################
def device_list(TokenAtual, OrganizationID):

	global assetName
	global assetID
	global plant
	global devices
	global assetGroupID
	global lastRead

	parameterAsset = {
	  "organizationID": OrganizationID,
	  "assetsWithoutGroup": "false"
	}

	requestAsset = requests.get(ABB_URL + ABB_ASSET_URL, headers=header, params=parameterAsset)

	if (requestAsset.status_code) == 200:
	  print("Lista de Ativos com sucesso!")
	else:
	  print("Erro {} ao requisitar Lista de Ativos!".format(requestAsset.status_code))

	requestResult = requestAsset.json()

	for x in requestResult:

		assetName = x["assetName"]
		assetID = x["assetID"]
		plant = x["plantName"]
		assetGroupID = x["assetGroupID"]
		lastRead = x["lastSyncTimeStamp"]
		d = dict(((k, eval(k)) for k in ("assetName", "assetID", "plant", "assetGroupID", "lastRead")))
		devices.append(d)

	#print(devices)

	return(devices)		
	
#########################  FUNÇÃO PARA LEITURA DOS DISPOSITIVOS  #########################
def device_read(TokenAtual, OrganizationID):

	global devices
	global value
	global timestamp
	global payload
	
	i = -1
	j = len(variables)-1
	indexPos = 0
	variable = 0
	a = -1
	b = len(devices_to_update)-1
	header = {"Authorization": TokenAtual, "FeatureCode": "EXT_AssetTrendData"}
					
	while (a < b):
		while (i < j):
			i += 1			
			initDay = init_days_list[indexPos]
			finalDay = str(dp.parse(final_days_list[indexPos]) + timedelta(hours=4))
			deviceIndex = devicesID_to_update[indexPos]
			deviceName = devices_Lables_ubidots[indexPos]
			MeasurementType = variables[i]						
			
			print("\n")
			print(MeasurementType, "- valor de MeasurementType")

			parameterMeas = {"from": initDay, "to": finalDay, "assetID": deviceIndex, "measurementTypes": MeasurementType}
					
			requestMeas = requests.get(ABB_URL + "/Measurement/Value", headers=header, params=parameterMeas)

			if (requestMeas.status_code) == 200:
				print("Lista de Ativos com sucesso!")
			else:
				print("Erro {} ao requisitar Lista de Ativos!".format(requestMeas.status_code))

			response = (requestMeas)
			measures = json.loads(response.text)[0]
			values = measures['measurements']
			payload=list()

			for x in values:
				try:
					value = x['measurementValue']
					t = x['measurementCreated']
					parsed_t = dp.parse(t) - timedelta(hours=3)
					timestamp = time.mktime(parsed_t.timetuple())*1000
					d = dict(((k, eval(k)) for k in ('value', 'timestamp')))
					payload.append(d)
				except:
					pass

			if MeasurementType==2:
				variable = "velocidade"    	
			if MeasurementType==4:
				variable = "temperatura_superficial"
			if MeasurementType==8:
				variable = "vibracao_global"
			if MeasurementType==9:
				variable = "tempo_operacao"
			if MeasurementType==10:
				variable = "numero_partidas"
			if MeasurementType==15:
				variable = "frequencia"
			if MeasurementType==27:
				variable = "condicao_rolamento"
			if MeasurementType==31:
				variable = "vibracao_radial"
			if MeasurementType==32:
				variable = "vibracao_tangencial"
			if MeasurementType==33:
				variable = "vibracao_axial"
			if MeasurementType==64:
				variable = "potencia_saida"
			if MeasurementType==65:
				variable = "relubrificacao"
			if MeasurementType==66:
				variable = "disalinhameto_simples"
			if MeasurementType==67:
				variable = "partidas_entre_medicoes"
			if MeasurementType==208:
				variable = "carga"
			if MeasurementType==209:
				variable = "tempo_operacao"	
	
			print(variable, "- Nome da variavel a ser atualizada")

			dName = deviceName 
			print(dName, "- Nome do dispositivo que está atualizando")

			try:
				url = "{}{}/{}/values".format(UBIDOTS_ENDPOINT, dName, variable)
				attempts = 0
				status_code = 400

				while status_code >= 400 and attempts < 5:
					print("[INFO] Enviando dados, numero de tentativas: {}".format(attempts))				
					req = requests.post(url=url, headers=header_ubidots, json=payload)
					status_code = req.status_code
					attempts += 1
					
				print("[INFO] Results: ", req.text)
			except Exception as e:
				print("[ERROR] Error posting, details: {}".format(e))
					
			if (MeasurementType == 64):	
				MeasurementType = 0
				indexPos += 1
				i = -1
				a += 1	
				if (indexPos >= 6):
					indexPos = 0			
				print("\n")
				print("############################")					
				print(devices_Lables_ubidots[indexPos])
				print("############################")
				print("\n")			
				break
		if (a == b):
			linksArrays_ubidots.clear()
			linksArrays_ABB.clear()
			last_timestamps_list_ubidots.clear()
			last_timestamps_list_ABB.clear()
			init_days_list.clear()
			final_days_list.clear()
			devices_to_update.clear()
			devicesID_to_update.clear()
			a = 0
			time.sleep(3600000)		
			break		

def device_events(TokenAtual, organizationID):
	
	global devices
	global value
	global timestamp
	global payload

	device = [ sub['assetID'] for sub in devices ]
	
	i = -1
	j = len(init_days_list)-1
	while (i < j):
		i += 1
		for index in device:
			#res = [d for d in devices if d['assetID'] == index]

			initDay = init_days_list[i]

			finalDay = final_days_list[i]

			delta = timedelta(days=1)

			while initDay <= finalDay:
				parameterEvents = {
					"eventLogType": "0",
					"from": initDay,
					"to": finalDay,
					"assetID": index,
					"filterClosed": "false",
					"organizationID": OrganizationID
					}

				requestEvents = requests.get(ABB_URL + ABB_EVENT_URL, headers=header, params=parameterEvents)

				if (requestEvents.status_code) == 200:
					print("Evento requisitada com sucesso!")
				else:
					print("Erro {} ao requisitar evento!".format(requestEvents.status_code))

				result = requestEvents.json()

				initDay+=delta

				payload=list()

				if len(result) == 0:				
					value = 0
					timestamp = initDay - timedelta(hours=3)
					timestamp = time.mktime(timestamp.timetuple())*1000
					d = dict(((k, eval(k)) for k in ('value', 'timestamp')))
					payload.append(d)

					event_send(payload, index)
			
				else:				
					for x in result:					
						value = x['countOfEventLogs']
						timestamp = x['eventLogCreatedOn']
						timestamp = dp.parse(timestamp) - timedelta(hours=3)
						timestamp = timestamp.date()
						d = dict(((k, eval(k)) for k in ('value', 'timestamp')))
						payload.append(d)

					payloadf=list()

					dcounts = Counter(d["timestamp"] for d in payload)
				
					for d, count in dcounts.items():					
						d = datetime.combine(d, datetime.min.time())
						value = count
						t = d
						timestamp = time.mktime(t.timetuple())*1000
						p = dict(((k, eval(k)) for k in ('value', 'timestamp')))
						payloadf.append(p)

					event_send(payloadf, index)

def device_health(TokenAtual, organizationID):
	health_send()

def data_send(TokenAtual, UBIDOTS_ENDPOINT, UBIDOTS_TOKEN, MeasurementType, deviceName):
         
	
	variable = 0

	if MeasurementType==2:
		variable = "velocidade"    	
	elif MeasurementType==4:
		variable = "temperatura_superficial"
	elif MeasurementType==8:
		variable = "vibracao_global"
	elif MeasurementType==9:
		variable = "tempo_operacao"
	elif MeasurementType==10:
		variable = "numero_partidas"
	elif MeasurementType==15:
		variable = "frequencia"
	elif MeasurementType==27:
		variable = "condicao_rolamento"
	elif MeasurementType==31:
		variable = "vibracao_radial"
	elif MeasurementType==32:
		variable = "vibracao_tangencial"
	elif MeasurementType==33:
		variable = "vibracao_axial"
	elif MeasurementType==64:
		variable = "potencia_saida"
	elif MeasurementType==65:
		variable = "relubrificacao"
	elif MeasurementType==66:
		variable = "disalinhameto_simples"
	elif MeasurementType==67:
		variable = "partidas_entre_medicoes"
	elif MeasurementType==208:
		variable = "carga"
	elif MeasurementType==209:
		variable = "tempo_operacao"

	
	print(variable, "- Nome da variavel a ser atualizada")

	dName = deviceName 
	print(dName, "- Nome do dispositivo que está atualizando")

	try:
		url = "{}{}/{}/values".format(UBIDOTS_ENDPOINT, dName, variable)
		attempts = 0
		status_code = 400

		while status_code >= 400 and attempts < 5:
			print("[INFO] Enviando dados, numero de tentativas: {}".format(attempts))				
			req = requests.post(url=url, headers=header_ubidots, json=payload)
			status_code = req.status_code
			attempts += 1

		print("[INFO] Results: ", req.text)
	except Exception as e:
		print("[ERROR] Error posting, details: {}".format(e))

def event_send(EVENT_PAYLOAD, index):
    
    global devices

    device = next((item for item in devices if item['assetID'] == index), None)
    deviceName = device["assetName"]

    try:
        url = "{}{}/eventos/values".format(UBIDOTS_ENDPOINT,deviceName)

        attempts = 0
        status_code = 400

        while status_code >= 400 and attempts < 5:
            print("[INFO] Sending data, attempt number: {}".format(attempts))
            req = requests.post(url=url, headers=header,
                                json=EVENT_PAYLOAD)
            status_code = req.status_code
            attempts += 1
            #time.sleep(1)

        print("[INFO] Results:")
        print(req.text)
    except Exception as e:
        print("[ERROR] Error posting, details: {}".format(e))

def health_send():
	pass


while True:
	generate_Token()
	organization_list(TokenAtual)
	getSyncs(TokenAtual)
	comparator()
	device_read(TokenAtual, OrganizationID)
	continue
	


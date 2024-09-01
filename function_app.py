from datetime import datetime
import json
import os
import logging
import azure.functions as func
from azure.data.tables import TableServiceClient

# Configura la conexión con la cuenta de almacenamiento
STORAGE_CONNECTION_STRING =  os.getenv("AZURE_STORAGE_CONNECTION_STRING")
TABLE_NAME = "UserNoFap"

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="no_fap_login", methods=["POST"])
def http_trigger_login(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    if req.method != "POST":
        return func.HttpResponse(
            "Method not allowed",
            status_code=405
        )

    try:
        req_body = req.get_json()
        entitie=insertarUsuario(req_body)
        print(req_body)
    except ValueError:
        return func.HttpResponse(
            "Invalid JSON",
            status_code=400
        )

    return func.HttpResponse(
        json.dumps({"data": entitie}),
        status_code=200,
        mimetype="application/json"
    )

@app.route(route="no_fap_update", methods=["PUT"])
def http_trigger_update(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    if req.method != "PUT":
        return func.HttpResponse(
            "Method not allowed",
            status_code=405
        )

    try:
        req_body = req.get_json()
        entitie = actualizarRacha(req_body.get("identifier"))
    except ValueError:
        return func.HttpResponse(
            "Invalid JSON",
            status_code=400
        )

    return func.HttpResponse(
        json.dumps({"data": entitie}),
        status_code=200,
        mimetype="application/json"
    )

def conectTableStorage():
    # Conectar con el Table Storage
    table_service_client = TableServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
    table_client = table_service_client.get_table_client(table_name=TABLE_NAME)

    return table_client

def insertarUsuario(req_body):
    tableStorage = conectTableStorage()
    # Crear una entidad
    rowkey = req_body.get("email", req_body.get("phone"))
    user_entity = {
        "PartitionKey": "UserNoFap",
        "RowKey": rowkey,
        "name": req_body.get("name"),
        "email": req_body.get("email"),
        "phone": req_body.get("phone"),
        "racha": 1,
        "start_date": datetime.now().isoformat() + 'Z',
    }
    
    try:
        user = tableStorage.get_entity(partition_key="UserNoFap", row_key=rowkey)
        return {'name':user.get('name'),'racha':user.get("racha"),'email':user.get('email')}
    except:
        tableStorage.create_entity(entity=user_entity)
        return {"name": req_body.get("name"),"racha": 1,'email':user.get('email')}


def actualizarRacha(identifier):
    tableStorage = conectTableStorage()
    # Filtra registros por propiedad
    user = tableStorage.get_entity(partition_key="UserNoFap", row_key=identifier)
    user["racha"] += 1
    tableStorage.update_entity(entity=user)
    return {'email':user.get('email'),'racha':user.get("racha")}

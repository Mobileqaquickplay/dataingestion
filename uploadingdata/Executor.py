import time
import requests
import csv
from lib.config import config_details
import json

#config details 
tenant_url = config_details["tenant_url"]
workspace = config_details["workspace"]
product_id = config_details["product_id"]
url = f"https://{tenant_url}/api/v1/graphql"

def getActorName(token , actor_name , headers):
    payload = json.dumps({
    "query": "query getActorByName {\n  %s_actors(\n    where: {\n      content: {\n        title: { value_en: { _ilike: \"%s\" } }\n        product_id: { _eq: \"%s\" }\n      }\n    }\n    limit: 1\n  ) {\n    flm_id\n  }\n}"%(workspace,actor_name,product_id),
    "variables": {},
    })
    encode_payload = payload.encode("utf-8")
    response = requests.request("POST", url, headers=headers, data = encode_payload)
    res_dict = json.loads(response.text)
    return res_dict.get("data", {}).get("%s_actors"%(workspace), [])

def createActor(token , actor_name , headers):
    payload = json.dumps({
            "variables": {},
            "query": "mutation createActor {\n  insert_%s_actors(\n    objects: {\n      content: {\n        data: {\n          title: { data: { value_en: \"%s\" } }\n          product_id: \"%s\"\n          service_id: \"663a5df1-2116-4862-a62d-38e47093ffbf\"\n          content_type_id: \"34e43369-1a03-4adb-967a-44e3f50679db\"\n        }\n      }\n    }\n  ) {\n    returning {\n      flm_id\n    }\n  }\n}\n"%(workspace,actor_name,product_id),
        })
    encoded_payload = payload.encode("utf-8")
    response = requests.request("POST", url, headers=headers, data=encoded_payload)
    if (response.status_code == 200 and "errors" not in response.text):
        res = json.loads(response.text)
        flm_id = res.get("data").get('insert_%s_actors'%(workspace)).get('returning')[0].get('flm_id')
        return flm_id
    return "failed to insert data into cms"

def getDirectorName(token ,directors_name , headers):
    payload = json.dumps({
    "query": "query getDirectorByName {\n  %s_directors(\n    where: {\n      content: {\n        title: { value_en: { _ilike: \"%s\" } }\n        product_id: { _eq: \"%s\" }\n      }\n    }\n    limit: 1\n  ) {\n    flm_id\n  }\n}"%(workspace , directors_name , product_id),
    "variables":{},
    })
    encode_payload = payload.encode("utf-8")
    response = requests.request("POST", url, headers=headers, data = encode_payload)
    res_dict = json.loads(response.text)
    return res_dict.get("data" , {}).get("%s_directors"%(workspace),[])

def createDirector(token , directors_name , headers):
    payload = json.dumps({
            "variables": {},"query": "mutation createDirector {\n  insert_%s_directors(\n    objects: {\n      content: {\n        data: {\n          title: { data: { value_en: \"%s\" } }\n          product_id: \"%s\"\n          service_id: \"663a5df1-2116-4862-a62d-38e47093ffbf\"\n          content_type_id: \"52f2bd26-4238-4b35-bdad-cc2b24f7aaa9\"\n        }\n      }\n    }\n  ) {\n    returning {\n      flm_id\n    }\n  }\n}"%(workspace,directors_name,product_id),
            "operationName": "createDirector"
            })
    encoded_payload = payload.encode("utf-8")
    response = requests.request("POST", url, headers=headers, data=encoded_payload)
    if (response.status_code == 200 and "errors" not in response.text):
        res = json.loads(response.text)
        flm_id = res.get("data").get('insert_%s_directors'%(workspace)).get('returning')[0].get('flm_id')
        return flm_id
    return "failed to insert data into cms"

def isMovieExist(token , movie_name , headers):
    payload = json.dumps({
    "query": "query isMovieExists {\n  %s_movies_aggregate(where: {content: {title: {value_en: {_eq: \"%s\"}}}}) {\n    aggregate {\n      count\n    }\n  }\n}"%(workspace,movie_name),
    "variables": {},
    })
    encoded_payload = payload.encode("utf-8")
    response = requests.request("POST", url, headers=headers, data = encoded_payload)
    res_dict = json.loads(response.text)
    count = res_dict["data"]["%s_movies_aggregate"%(workspace)]["aggregate"]["count"]
    return count != 0 

def isTvSeriesExit(token , tvseries_name ,headers):
    payload = json.dumps({
    "query": "query isTvSeriesExists {\n  %s_tvseries_aggregate(where: {content: {title: {value_en: {_eq: \"%s\"}}}}) {\n    aggregate {\n      count\n    }\n  }\n}"%(workspace,tvseries_name),
    "variables": {},
    })
    encoded_payload  = payload.encode("utf-8")
    response = requests.request("POST", url, headers=headers, data = encoded_payload)
    res_dict = json.loads(response.text)
    return res_dict
    




import time
import requests
import csv
import multiprocessing
import sys
from lib.auth import generate_idp_token
from lib.config import config_details
import json
import logging
import Executor

#config details 
apiKey = config_details["apiKey"]
user_id = config_details["user_id"]
tenant_id = config_details["tenant_id"]
tenant_url = config_details["tenant_url"]
workspace = config_details["workspace"]
sa_key = config_details["sa_key"]
product_id = config_details["product_id"]

#num of process
num_processes = 30

#movies_id_file
movies_id_file = "result/movies_id.csv"
tvseries_id_file = "result/tvseries_id.csv"
actors_dict = {}
directors_dict = {}
token_data = {'last_token': None, 'last_token_time': None}
def generate_or_refresh_token():
    current_time = time.time()
    if token_data['last_token'] is not None and token_data['last_token_time'] is not None:
        if (current_time - token_data['last_token_time']) >= 3600:
            # Generate a new token after 1 hour
            token = generate_idp_token(apiKey, tenant_id, user_id, sa_key, tenant_url)
            token_data['last_token'] = token
            token_data['last_token_time'] = current_time
        else:
            token = token_data['last_token']
    else:
        # Generate a new token for the first time
        token = generate_idp_token(apiKey, tenant_id, user_id, sa_key, tenant_url)
        token_data['last_token'] = token
        token_data['last_token_time'] = current_time
    return token

def process_chunk(chunk):
    url = f"https://{tenant_url}/api/v1/graphql"
    token = generate_idp_token(apiKey, tenant_id, user_id, sa_key, tenant_url)
    headers = {"Authorization": f"Bearer {token}", "x-qp-authorization": token}
    for row in chunk:
        imdbID,Title,Year,Type,Actors,Director,Plot,Poster,Ratings = row
        if(len(Ratings) == 0):
            Ratings = "6.7/10"
        rating = Ratings.strip("/10")
        if(Actors == "N/A" or Director == "N/A"):
            continue
        '''
        if(Executor.isMovieExist(token = token , movie_name = Title , headers = headers)):
            print("movies details is already present in cms")
            continue
        '''
        try:
            count = 0
            tvseries_data = Executor.isTvSeriesExit(token ,Title ,headers)
            if("errors" in tvseries_data and 'JWTExpired' in tvseries_data['errors'][0]['message']):
                token = generate_or_refresh_token()
            else:
                count = tvseries_data["data"]["%s_tvseries_aggregate"%(workspace)]["aggregate"]["count"]
            if(count != 0):
                print("tvseries details is already present in cms")
                continue
        except Exception as e:
            print("exception occured{}".format(e))


        #processing the actors content_type 
        #checking the actor details in cms 
        try:
            actors_data = Executor.getActorName(token = token , actor_name = Actors ,  headers = headers)
            if(len(actors_data) == 0):
                flm_id = Executor.createActor(token = token , actor_name = Actors ,  headers = headers)
                actors_dict[Actors] = flm_id
            else:
                print("actors data is already present in cms")
                actors_dict[Actors] = actors_data[0].get("flm_id")

        except Exception as e :
            print("error capture{}".format(e))
            token = generate_or_refresh_token()
        
        #processing the directors content type
        #checking the directros details in cms 
        try:
            directors_data = Executor.getDirectorName(token = token , directors_name = Director , headers = headers)
            if(len(directors_data) == 0):
                flm_id = Executor.createDirector(token = token , directors_name = Director , headers = headers)
                directors_dict[Director] = flm_id
            else:
                print("director data is already present in cms")
                directors_dict[Director] = directors_data[0].get("flm_id")
        except Exception as e:
            print("error capture{}".format(e))
            token = generate_or_refresh_token()
        #processing the movies content_type 
        #checking the movie details in csv
        # movies content details updation in cms  
        '''
        try:
            payload = json.dumps({
            "query": "mutation createMovies {\n  insert_%s_movies(\n    objects: {\n      description: \"%s\"\n      rating: \"%s\"\n      year: \"%s\"\n      movies_actors: {\n        data: [{ actors_id: \"%s\" }] \n      }\n      movies_directors: {\n        data: [{ directors_id: \"%s\" }] \n      }\n      content: {\n        data: {\n          product_id: \"%s\"\n          service_id: \"663a5df1-2116-4862-a62d-38e47093ffbf\"\n          content_type_id: \"7757be9d-2e42-43d2-8339-0872368d527d\"\n          title: { data: { value_en: \"%s\" } }\n          content_external_id: {data: {imdb: \"%s\"}}\n          content_image_asset: {\n            data: {\n              media_asset: {\n                data: {\n                  asset_type_id: \"1fbd4d22-c1d2-41d0-b9b7-67a9da201684\", \n                  uri: \"%s\", \n                  asset: {\n                    imageUri: \"%s\", \n                    imageName: \"%s.png\", \n                    imageType: \"\", \n                    aspectRatio: \"79x15\", \n                    megaBytes: 0.04078, \n                    resolution: \"832x158\", \n                    imageTags: {}\n                  }\n                }\n              }\n            }\n          }\n        }\n      }\n    }\n  ) {\n    affected_rows\n    returning {\n      flm_id \n      movies_actors_aggregate {\n        aggregate {\n          count\n        }\n      }\n      movies_directors_aggregate {\n        aggregate {\n          count\n        }\n      }\n    }\n  }\n}"%(workspace,Plot,rating,Year,actors_dict[Actors],directors_dict[Director],product_id,Title,imdbID,Poster,Poster,Title),
            "variables": {},
            })
            encode_payload = payload.encode("utf-8")
            response = requests.request("POST", url, headers=headers, data = encode_payload)
            if (response.status_code == 200 and "errors" not in response.text):
                data_dict = json.loads(response.text)
                flm_id = data_dict.get("data", {}).get("insert_%s_movies"%(workspace), {}).get("returning", [{}])[0].get("flm_id")
                with open(movies_id_file, 'a' , newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow([Title,flm_id])
                print("sucessfully captured the flm_id")
            else:
                print(f"Response contains error object: {response.text}")
                with open("result/error.csv" , 'a' , newline= '') as file:
                    writer = csv.writer(file)
                    writer.writerow(row)
        except Exception as e:
            print("exeption occured {}".format(e))
        '''
        #tvseries details updation on cms 
        try:
            payload = json.dumps({
            "query": "mutation createTvseries {\n  insert_%s_tvseries(\n  objects: {\n\tdescription: \"tv series %s\"\n\trating: \"%s\"\n\tyear: \"%s\" \n\ttvseries_actors: {data: {actors_id: \"%s\"}}\n\ttvseries_directors: {data: {directors_id: \"%s\"}}\n\t\tcontent: {\n\t\tdata: \n\t\t\t{product_id: \"%s\"\n\t\t\tservice_id: \"663a5df1-2116-4862-a62d-38e47093ffbf\"\n\t\t\tcontent_type_id: \"a3e1dcd8-940a-46aa-9b48-31c501a2e0cc\"\n\t\t\ttitle: {data: {value_en: \"%s\"}}\n\t\t\tcontent_external_id: {data: {imdb:\"%s\"}}\n\t\t\tcontent_image_asset: {\n\t\t\t\tdata: {\n\t\t\t\t\tmedia_asset: {\n\t\t\t\t\t\tdata: {\n\t\t\t\t\t\t\tasset_type_id: \"1fbd4d22-c1d2-41d0-b9b7-67a9da201684\"\n\t\t\t\t\t\t\turi: \"%s\" \n\t\t\t\t\t\t\tasset: {\n\t\t\t\t\t\t\timageUri: \"%s\", \n\t\t\t\t\t\t\timageName: \"%s.png\", \n\t\t\t\t\t\t\timageType: \"\", \n\t\t\t\t\t\t\taspectRatio: \"79x15\", \n\t\t\t\t\t\t\tmegaBytes: 0.04078, \n\t\t\t\t\t\t\tresolution: \"832x158\", \n\t\t\t\t\t\t\timageTags: {}\n\t\t\t\t\t\t\t}\n\t\t\t\t\t\t}\n\t\t\t\t\t}\n\t\t\t\t}\n\t\t\t}\n\t\t}\n\t}\n}\n) {\n    affected_rows\n    returning {\n      flm_id\n      tvseries_actors_aggregate {\n        aggregate {\n          count\n        }\n      }\n      tvseries_directors_aggregate {\n        aggregate {\n          count\n        }\n      }\n    }\n  }\n}\n"%(workspace,Plot,rating,Year,actors_dict[Actors],directors_dict[Director],product_id,Title,imdbID,Poster,Poster,Title),
            "variables": {},
            })
            encode_payload = payload.encode("utf-8")
            response = requests.request("POST", url, headers=headers, data = encode_payload)
            if (response.status_code == 200 and "errors" not in response.text):
                data_dict = json.loads(response.text)
                flm_id = data_dict.get("data", {}).get("insert_%s_tvseries"%(workspace), {}).get("returning", [{}])[0].get("flm_id")
                with open(tvseries_id_file, 'a' , newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow([Title,flm_id])
                print("sucessfully captured the flm_id")
            else:
                print(f"Response contains error object: {response.text}")
                with open("result/tvseries_error.csv" , 'a' , newline= '') as file:
                    writer = csv.writer(file)
                    writer.writerow(row)
        except Exception as e:
            print("exeption occured {}".format(e))

if __name__ == "__main__":
    last_token = generate_idp_token(apiKey, tenant_id, user_id, sa_key, tenant_url)
    last_token_time = time.time()
    #reading the movies test data 
    try:
        with open('data/tvseries.csv', mode='r', encoding='utf-8') as movies_file:
            movies_data = list(csv.reader(movies_file))
    except Exception as e:
        print("exception occured due to {e}")
    # Split data into groups  for parallel processing
    chunk_size = len(movies_data) // num_processes
    chunks = [movies_data[i:i + chunk_size] for i in range(0, len(movies_data), chunk_size)]

    with multiprocessing.Pool(processes=num_processes) as pool:
        pool.map(process_chunk, chunks)
    

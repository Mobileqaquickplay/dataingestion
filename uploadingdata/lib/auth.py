import firebase_admin
import requests
from firebase_admin import credentials, tenant_mgt

def generate_idp_token(apikey, tenant_id, user_id, sa_key, tenant_url):
    
    # firebase setup
    # initialize firebase app
    cred = credentials.Certificate(sa_key)
    app = firebase_admin.initialize_app(cred)

    # create firebase client
    tenant_auth = tenant_mgt.auth_for_tenant(tenant_id)

    # generate custom_token by user_id
    custom_token = tenant_auth.create_custom_token(user_id).decode()
    # clean up firebase app
    firebase_admin.delete_app(app)

    # request IDP token
    # https://cloud.google.com/identity-platform/docs/reference/rest/v1/accounts/signInWithCustomToken
    json_body = {
        "token": custom_token,
        "returnSecureToken": True,
        "tenantId": tenant_id,
    }
    url = f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithCustomToken?key={apikey}"
    resp = requests.post(url, json=json_body)
    idp_token = resp.json()["idToken"]

    #exchange for custom token
    url = f"https://{tenant_url}/cms-authz/getToken"
    try:
        resp = requests.get(url, headers={"Authorization": f"Bearer {idp_token}"})
        token = resp.json()["idToken"]
        return token
    except Exception as e:
        print("Unable to exchange for custom token", f"http {resp.status_code} {resp.text}", e)
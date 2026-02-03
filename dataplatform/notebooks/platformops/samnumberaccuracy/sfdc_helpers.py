import requests

SFDC_CLIENT_ID = "3MVG9sG9Z3Q1RlbeiSTfYMXB6r99PsL8XE13WDQhOcJEzlgDo_L8mDpK1sqgIrThSkfynpErK8cAJ4M9zyjVU"
SFDC_USERNAME = "sfdc-cloud-integration@samsara.com"

SFDC_API_VERSION = "39.0"
SOQL_ENDPOINT = f"/services/data/v{SFDC_API_VERSION}/query/"

SFDC_PASSWORD = dbutils.secrets.get(scope="platform-operations", key="sfdc_password")
SFDC_SECRET = dbutils.secrets.get(scope="platform-operations", key="sfdc_secret")


class SfdcConnection:
    def __init__(self, *, debug=False):
        self.debug = debug

        # Setup Token
        params = {
            "grant_type": "password",
            "client_id": SFDC_CLIENT_ID,  # Consumer Key
            "client_secret": SFDC_SECRET,  # Consumer Secret
            "username": SFDC_USERNAME,  # The email you use to login
            "password": SFDC_PASSWORD,  # Concat your password and your security token
        }
        # Setup Access Token
        r = requests.post(
            "https://login.salesforce.com/services/oauth2/token", params=params
        )
        self.access_token = r.json().get("access_token")
        self.instance_url = r.json().get("instance_url")

    # sf_api_call calls out to SFDC and returns the JSON value
    def sf_api_call(self, action, parameters={}, data={}):
        """
        Helper function to make calls to Salesforce REST API.
        Parameters: action (the URL), URL params, method (get, post or patch), data for POST/PATCH.
        """
        headers = {
            "Content-type": "application/json",
            "Accept-Encoding": "gzip",
            "Authorization": "Bearer %s" % self.access_token,
        }
        r = requests.request(
            "get",
            self.instance_url + action,
            headers=headers,
            params=parameters,
            timeout=60,
        )
        if r.status_code < 300:
            return r.json()
        else:
            raise Exception("API error when calling %s : %s" % (r.url, r.content))

    # get_results_from_call returns the results of an API call for query
    def get_results_from_call(self, query, *, dev=False):
        call = self.sf_api_call(SOQL_ENDPOINT, {"q": query})
        rows = call.get("records", [])
        next = call.get("nextRecordsUrl", None)
        while next:
            if len(rows) % 10000 == 0:
                if self.debug:
                    print("...have %d records so far" % len(rows))

            # Return early while developing queries
            if dev and len(rows) >= 1000:
                return rows

            call = self.sf_api_call(next)
            rows.extend(call.get("records", []))
            next = call.get("nextRecordsUrl", None)
        if self.debug:
            print("Finished fetching %d records" % len(rows))
        return rows


# describe_object_api returns the results of an API query for the description of a SOQL Object.
# This is helpful for building queries
def describe_object_api(self, object):
    call = self.sf_api_call(f"/services/data/v39.0/sobjects/{object}/describe")
    return call

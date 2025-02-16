class AzureAISearchClient:
    def create_indexer(self, name, datasource_name, index_name):
        url = f"{self.endpoint}/indexers/{name}?api-version={self.api_version}"
        
        # First, let's verify the indexer doesn't already exist
        try:
            check_url = f"{self.endpoint}/indexers/{name}?api-version={self.api_version}"
            check_response = requests.get(check_url, headers=self.headers)
            if check_response.status_code == 200:
                print(f"Indexer {name} already exists. Deleting it first...")
                delete_response = requests.delete(check_url, headers=self.headers)
                print(f"Delete response status: {delete_response.status_code}")
        except Exception as e:
            print(f"Error checking existing indexer: {str(e)}")

        # Prepare the indexer payload
        payload = {
            "name": name,
            "dataSourceName": datasource_name,
            "targetIndexName": index_name,
            "fieldMappings": [
                {
                    "sourceFieldName": "content",
                    "targetFieldName": "content"
                },
                {
                    "sourceFieldName": "id",
                    "targetFieldName": "id"
                },
                {
                    "sourceFieldName": "title",
                    "targetFieldName": "title"
                },
                # Add other necessary field mappings
            ],
            "parameters": {
                "configuration": {
                    "dataToExtract": "contentAndMetadata",
                    "parsingMode": "default"
                }
            }
        }

        try:
            # Make the request with extended timeout
            response = requests.put(
                url,
                headers=self.headers,
                json=payload,
                timeout=60  # Extended timeout
            )
            
            # Print detailed request information
            print("\nRequest Details:")
            print(f"URL: {url}")
            print(f"Headers: {self.headers}")
            print(f"Payload: {json.dumps(payload, indent=2)}")
            
            # Print response information
            print("\nResponse Details:")
            print(f"Status Code: {response.status_code}")
            print(f"Headers: {dict(response.headers)}")
            print(f"Content: {response.content.decode('utf-8') if response.content else 'No content'}")
            
            # Try to get more information if available
            try:
                response_json = response.json()
                print(f"\nJSON Response: {json.dumps(response_json, indent=2)}")
            except:
                print("No JSON response available")
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.ConnectionError as e:
            print("\nConnection Error Details:")
            print(f"Error Type: {type(e).__name__}")
            print(f"Error Message: {str(e)}")
            
            # Try to verify the service is reachable
            try:
                test_url = f"{self.endpoint}/indexes?api-version={self.api_version}"
                test_response = requests.get(test_url, headers=self.headers, timeout=10)
                print(f"\nService reachability test status code: {test_response.status_code}")
            except Exception as test_e:
                print(f"Service reachability test failed: {str(test_e)}")
            
            raise
            
        except Exception as e:
            print(f"\nUnexpected error: {str(e)}")
            raise

# Check if datasource exists and is accessible
datasource_url = f"{self.endpoint}/datasources/{datasource_name}?api-version={self.api_version}"
datasource_response = requests.get(datasource_url, headers=self.headers)
print(f"Datasource check status: {datasource_response.status_code}")
print(f"Datasource content: {datasource_response.content}")


# Check if index exists and is accessible
index_url = f"{self.endpoint}/indexes/{index_name}?api-version={self.api_version}"
index_response = requests.get(index_url, headers=self.headers)
print(f"Index check status: {index_response.status_code}")
print(f"Index content: {index_response.content}")

simple_payload = {
    "name": name,
    "dataSourceName": datasource_name,
    "targetIndexName": index_name,
    "fieldMappings": [
        {
            "sourceFieldName": "id",
            "targetFieldName": "id"
        }
    ]
}



def create_indexer(self, name, datasource_name, index_name):
    url = f"{self.endpoint}/indexers/{name}?api-version={self.api_version}"
    
    simple_payload = {
        "name": name,
        "dataSourceName": datasource_name,
        "targetIndexName": index_name,
        "fieldMappings": [
            {
                "sourceFieldName": "id",
                "targetFieldName": "id"
            }
        ]
    }
    
    # Create a session with specific configurations
    session = requests.Session()
    
    # Configure the session
    adapter = requests.adapters.HTTPAdapter(
        max_retries=3,
        pool_connections=1,
        pool_maxsize=1
    )
    session.mount('https://', adapter)
    
    try:
        # Use session with specific configurations
        response = session.put(
            url,
            headers=self.headers,
            json=simple_payload,
            timeout=30,
            verify=True,
            allow_redirects=True,
            stream=False  # Disable streaming
        )
        
        print(f"Response Status: {response.status_code}")
        print(f"Response Content: {response.content}")
        
        return response.json()
        
    except requests.exceptions.ConnectionError as e:
        print(f"Connection Error: {str(e)}")
        raise
    finally:
        session.close()  # Ensure the session is closed



import urllib3
import json
from urllib3.util.retry import Retry

def create_indexer_alternative(self, name, datasource_name, index_name):
    url = f"{self.endpoint}/indexers/{name}?api-version={self.api_version}"
    
    simple_payload = {
        "name": name,
        "dataSourceName": datasource_name,
        "targetIndexName": index_name,
        "fieldMappings": [
            {
                "sourceFieldName": "id",
                "targetFieldName": "id"
            }
        ]
    }
    
    # Create a pool manager with retry strategy
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.5
    )
    
    http = urllib3.PoolManager(
        retries=retry_strategy,
        maxsize=1,
        timeout=urllib3.Timeout(connect=5, read=30)
    )
    
    try:
        encoded_data = json.dumps(simple_payload).encode('utf-8')
        
        response = http.request(
            'PUT',
            url,
            body=encoded_data,
            headers=self.headers
        )
        
        print(f"Response Status: {response.status}")
        print(f"Response Data: {response.data}")
        
        return json.loads(response.data.decode('utf-8'))
        
    except Exception as e:
        print(f"Error: {str(e)}")
        raise
    finally:
        http.clear()

curl -X PUT "https://YOUR-SERVICE-NAME.search.windows.net/indexers/YOUR-INDEXER-NAME?api-version=2023-11-01" \
     -H "Content-Type: application/json" \
     -H "api-key: YOUR-ADMIN-KEY" \
     -d '{"name":"YOUR-INDEXER-NAME","dataSourceName":"YOUR-DATASOURCE","targetIndexName":"YOUR-INDEX","fieldMappings":[{"sourceFieldName":"id","targetFieldName":"id"}]}'


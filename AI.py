def diagnose_datasource(self, datasource_name):
    """
    Diagnoses issues with an Azure Search data source by performing various checks.
    
    Args:
        datasource_name (str): Name of the data source to diagnose
    """
    url = f"{self.endpoint}/datasources/{datasource_name}?api-version={self.api_version}"
    
    try:
        # Get the data source details
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        datasource = response.json()
        
        print("\n=== Data Source Diagnostic Report ===")
        
        # Check if data source exists
        print(f"\n1. Data Source Existence Check:")
        print(f"✓ Data source '{datasource_name}' exists")
        
        # Validate connection string
        print("\n2. Connection String Analysis:")
        connection_string = datasource.get('credentials', {}).get('connectionString', '')
        if not connection_string:
            print("❌ Connection string is empty")
        else:
            # Parse connection string
            try:
                conn_parts = dict(part.split('=', 1) for part in connection_string.split(';') if part)
                
                required_params = ['AccountName', 'AccountKey', 'DefaultEndpointsProtocol']
                for param in required_params:
                    if param not in conn_parts:
                        print(f"❌ Missing required parameter: {param}")
                    else:
                        print(f"✓ Found {param}")
                
                # Verify protocol
                if conn_parts.get('DefaultEndpointsProtocol', '').lower() not in ['https', 'http']:
                    print("❌ Invalid protocol in connection string")
                
            except Exception as e:
                print(f"❌ Invalid connection string format: {str(e)}")
        
        # Check container configuration
        print("\n3. Container Configuration:")
        container = datasource.get('container', {})
        if not container:
            print("❌ Container configuration is missing")
        else:
            if 'name' not in container:
                print("❌ Container name is missing")
            else:
                print(f"✓ Container name is specified: {container.get('name')}")
            
            if 'query' in container:
                print(f"✓ Container query is specified: {container.get('query')}")
        
        # Verify data source type
        print("\n4. Data Source Type Check:")
        datasource_type = datasource.get('type')
        valid_types = ['azureblob', 'azuretable', 'azuresql', 'cosmosdb']
        if not datasource_type:
            print("❌ Data source type is missing")
        elif datasource_type.lower() not in valid_types:
            print(f"❌ Invalid data source type: {datasource_type}")
        else:
            print(f"✓ Valid data source type: {datasource_type}")
        
        # Test connectivity
        print("\n5. Connectivity Test:")
        try:
            # This will vary depending on your data source type
            if datasource_type.lower() == 'azureblob':
                from azure.storage.blob import BlobServiceClient
                blob_service = BlobServiceClient.from_connection_string(connection_string)
                container_client = blob_service.get_container_client(container.get('name'))
                # List blobs to test access
                next(container_client.list_blobs(), None)
                print("✓ Successfully connected to blob container")
            else:
                print("ℹ Connectivity test not implemented for this data source type")
        except Exception as e:
            print(f"❌ Connectivity test failed: {str(e)}")
            
        # Additional metadata
        print("\n6. Additional Configuration:")
        print(f"- Description: {datasource.get('description', 'Not specified')}")
        print(f"- Data change detection policy: {datasource.get('dataChangeDetectionPolicy', 'Not specified')}")
        print(f"- Data deletion detection policy: {datasource.get('dataDeletionDetectionPolicy', 'Not specified')}")
        
        return datasource
        
    except requests.exceptions.RequestException as e:
        print(f"\n❌ Error accessing data source: {str(e)}")
        if hasattr(e.response, 'json'):
            try:
                error_details = e.response.json()
                print(f"Error details: {json.dumps(error_details, indent=2)}")
            except:
                print(f"Raw error response: {e.response.text}")
        raise








def verify_datasource_details(self, name):
    url = f"{self.endpoint}/datasources/{name}?api-version={self.api_version}"
    response = requests.get(url, headers=self.headers)
    print("Data Source Configuration:")
    print(json.dumps(response.json(), indent=2))
    return response.json()


def test_datasource_connection(self, name):
    url = f"{self.endpoint}/datasources/{name}/test?api-version={self.api_version}"
    response = requests.post(url, headers=self.headers)
    print("Connection Test Response:")
    print(response.content.decode())
    return response


def check_managed_identity(self):
    # Get the Search service details
    url = f"{self.endpoint}?api-version={self.api_version}"
    response = requests.get(url, headers=self.headers)
    print("Search Service Identity:")
    print(json.dumps(response.json(), indent=2))


# Run all checks
client.verify_datasource_details("your-datasource-name")
client.test_datasource_connection("your-datasource-name")
client.check_managed_identity()


payload = {
        "name": name,
        "type": "azureblob",
        "credentials": {
            "connectionString": f"ResourceId=/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Storage/storageAccounts/{storage_account_name}"
        },
        "container": {
            "name": container_name
        },
        "identity": {
            "type": "SystemAssigned"
        }
    }

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
     --http1.1 \
     -H "Content-Type: application/json" \
     -H "api-key: YOUR-ADMIN-KEY" \
     -d '{"name":"YOUR-INDEXER-NAME","dataSourceName":"YOUR-DATASOURCE","targetIndexName":"YOUR-INDEX","fieldMappings":[{"sourceFieldName":"id","targetFieldName":"id"}]}' \
     -v

curl -X PUT "https://YOUR-SERVICE-NAME.search.windows.net/indexers/YOUR-INDEXER-NAME?api-version=2023-11-01" \
     --http1.1 \
     --retry 3 \
     --retry-delay 2 \
     --max-time 30 \
     -H "Content-Type: application/json" \
     -H "api-key: YOUR-ADMIN-KEY" \
     -d '{"name":"YOUR-INDEXER-NAME","dataSourceName":"YOUR-DATASOURCE","targetIndexName":"YOUR-INDEX","fieldMappings":[{"sourceFieldName":"id","targetFieldName":"id"}]}' \
     -v

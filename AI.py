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

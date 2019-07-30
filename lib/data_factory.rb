require_relative 'my_azure_api.rb'

# Data Factory service Api calls
# This Class handles all operations for a Data Factory
class DATA_FACTORY
        include MyAzure

        private
            attr_reader :resource, :tenantId, :clientId
            attr_reader :clientSecret, :subscriptionId
            attr_reader :bearerToken, :resourceGroupName
    
        public
            # To initialize an instance of ADLS all the is need is the name of the data lake.
            # @param accountName [String] name of Azure Data Lake
            def initialize()
                @resource = MyAzure.get_resource
                @tenantId = MyAzure.get_tenant_id
                @clientId = MyAzure.get_client_id
                @clientSecret = MyAzure.get_client_secret
                @subscriptionId = MyAzure.get_subscription_id
                @resourceGroupName = MyAzure.get_resourceGroupName
                
                # Generate bearer token.
                @bearerToken = MyAzure.auth_bearer_management
            end
    
            # Get information for specified Data Lake Storage Gen1 account
            # Returns response code 200 ok if successfully retrieved account information
            def get_datafactory_info(name) 
                response = HTTParty.get("https://management.azure.com/subscriptions/#{subscriptionId}/resourceGroups/#{resourceGroupName}/providers/Microsoft.DataFactory/factories/#{name}?api-version=2018-06-01", {
    
                        headers: {
                            "Authorization" => "Bearer #{bearerToken}",
                            "Accept" => '*/*',
                            "Cache-Control" => 'no-cache',
                            "Connection" => 'keep-alive',
                            "cache-control" => 'no-cache'
                        },
                  
                        verify: true,
                })
    
                return  JSON.parse response.read_body
            end
    
            # Creates the specified Datafactory account
            # Returns response code 200 ok if successfully created account
            def create_datafactory(name) 
    
                factory_create = {
                    "location": "centralus"
                  }
    
                response = HTTParty.put("https://management.azure.com/subscriptions/#{subscriptionId}/resourceGroups/#{resourceGroupName}/providers/Microsoft.DataFactory/factories/#{name}?api-version=2018-06-01", {
    
                        body: factory_create.to_json,
    
                        headers: {
                            "Authorization" => "Bearer #{bearerToken}",
                            "Content-Type" => 'application/json', 
                            "Accept" => '*/*',
                            "Cache-Control" => 'no-cache',
                            "Connection" => 'keep-alive',
                            "cache-control" => 'no-cache'
                        },
                  
                        verify: true,
                })
    
                return  JSON.parse response.read_body
            end
    

end #End of Data Factory class
    
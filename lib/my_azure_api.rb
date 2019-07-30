require 'net/http'
require 'json'
require 'httparty'
require 'time'

# Module to access azure services.
# Has API calls for managing the file system on the data lake in the ADLS class.
# Has API calls for managing users, groups, and permission in the GRAPH class.
module MyAzure
    include HTTParty

    public
    # User must first call this function with the proper credentinals before making any API calls.
    # @param tenant [String] the tenant id for the Azure Data Lake
    # @param client [String] the client id for the Azure Data Lake
    # @param secret [String] the secret id for the Azure Data Lake
    # @param subscription [String] the subsription id that the Azure Data Lake is under
    # @return [JSON]
    def self.set_credentials(tenant, client, secret, subscription)
        @@tenantId = tenant
        @@clientId = client
        @@clientSecret = secret
        @@subscriptionId = subscription
    end

    # @param resourceGroupName [String] the resource group your services are in
    def self.set_resource_group(resourceGroupName)
        @@resourceGroupName = resourceGroupName
    end

    @@resource = "https://management.azure.com"

    def self.get_resource()
        @@resource
    end
    def self.get_tenant_id()
        @@tenantId
    end
    def self.get_client_id()
        @@clientId
    end
    def self.get_client_secret()
        @@clientSecret
    end
    def self.get_subscription_id()
        @@subscriptionId
    end

    def self.get_resourceGroupName()
        @@resourceGroupName
    end

    # Post request to login azure, returns bearer token 
    # to be used for authentication in Active Directory.
    def self.auth_bearer_graph
        response = HTTParty.get("https://login.microsoftonline.com/#{self.get_tenant_id}/oauth2/v2.0/token", {
            body: "grant_type=client_credentials&client_id=#{self.get_client_id}"+
                        "&client_secret=#{self.get_client_secret}"+
                         "&scope=https%3A%2F%2Fgraph.microsoft.com%2F.default"
        })
    
        parsed_json = JSON.parse response.read_body
        return parsed_json["access_token"]
    end

    # Post request to login azure, returns bearer token to be used for authentication.
    def self.auth_bearer_management
        response = HTTParty.get("https://login.microsoftonline.com" + "/#{self.get_tenant_id}/oauth2/token", {
            body: "grant_type=client_credentials&client_id=#{self.get_client_id}"+
                        "&client_secret=#{self.get_client_secret}"+
                        "&resource=https%3A%2F%2Fmanagement.azure.com%2F"+
                        "&Content-Type=application/x-www-form-urlencoded"
        })
        
        parsed_json = JSON.parse response.read_body
        return parsed_json["access_token"]
    end

end #end of MyAzure Module
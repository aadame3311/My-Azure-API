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

end #end of MyAzure Module
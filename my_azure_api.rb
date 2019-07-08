require 'net/http'
require 'json'

## Module to access azure services.
module MyAzure

    @@resource = "https://management.azure.com"

    # User must first call this function with the right params.
    def self.set_credentials(tenant, client, secret, subscription)
        @@tenantId = tenant
        @@clientId = client
        @@clientSecret = secret
        @@subscriptionId = subscription
    end

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


    ## ADLS Gen 1 service.
    class ADLS 
    private
        attr_reader :resource, :tenantId, :clientId
        attr_reader :clientSecret, :subcriptionId
        attr_reader :bearerToken, :accountName

        # Post request to login azure, returns bearer token 
        # to be used for authentication.
        def auth_bearer!
            url = "https://login.microsoftonline.com/#{tenantId}/oauth2/token"
            uri = URI.parse(url)
            
            http = Net::HTTP.new(uri.host, uri.port)
            http.use_ssl = true
            req = Net::HTTP::Post.new(uri)

            req.body = "grant_type=client_credentials&client_id=#{clientId}"+
                "&client_secret=#{clientSecret}"+
                "&resource=https%3A%2F%2Fmanagement.azure.com%2F"

            puts "contacting #{url}"
            res = http.request(req)

            # Handle responses.
            case res
            when Net::HTTPSuccess then
                puts "Success: bearer token generated."
            when Net::HTTPError then
                puts "Error: token could not be generated"
                return
            end
            parsed_json = JSON.parse(res.read_body)
            return parsed_json["access_token"]
        end

    public
        def initialize(accountName)
            @accountName = accountName
            @resource = MyAzure.get_resource
            @tenantId = MyAzure.get_tenant_id
            @clientId = MyAzure.get_client_id
            @clientSecret = MyAzure.get_client_secret
            @subscriptionId = MyAzure.get_subscription_id
            
            # Generate bearer token.
            @bearerToken = auth_bearer!
        end

        def list_status(dir) 

            url = "https://#{accountName}.azuredatalakestore.net" +
                "/webhdfs/v1/#{dir}?op=LISTSTATUS"
            uri = URI.parse(url)
            http = Net::HTTP.new(uri.host, uri.port)
            req = Net::HTTP::Get.new(uri)

            http.use_ssl = true
            bearerToken.strip!

            req["Authorization"] = "Bearer #{bearerToken}"
            req["Accept"] = '*/*'
            req["Cache-Control"] = 'no-cache'
            req["Host"] = "#{accountName}.azuredatalakestore.net"
            req["Connection"] = 'keep-alive'
            req["cache-control"] = 'no-cache'

            puts "contacting #{uri}"
            res = http.request(req)
            
            # Handle responses.
            case res
            when Net::HTTPError then
                puts "Error: files could not be retrieved."
                return
            end

            parsed_json = JSON.parse(res.read_body)
            return parsed_json
        end
    end
end


##################### TESTING #####################
_tId = ENV['AZURE_TENANT_ID']
_cId = ENV['AZURE_CLIENT_ID']
_scrt = ENV['AZURE_CLIENT_SECRET']
_sub = ENV['AZURE_SUBSCRIPTION_ID']

MyAzure.set_credentials(_tId, _cId, _scrt, _sub)
adls = MyAzure::ADLS.new("r1dltest")
files = adls.list_status("") # list files on root directory..

# list all file names under directory.
puts files
###################################################


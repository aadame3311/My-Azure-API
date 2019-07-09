require 'net/http'
require 'json'
require 'httparty'
require 'time'

## Module to access azure services.
module MyAzure
    include HTTParty
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
        def auth_bearer
            response = HTTParty.get("https://login.microsoftonline.com"+
                "/#{tenantId}/oauth2/token", {
                    body: "grant_type=client_credentials&client_id=#{clientId}"+
                        "&client_secret=#{clientSecret}"+
                        "&resource=https%3A%2F%2Fmanagement.azure.com%2F"
            })

            parsed_json = JSON.parse response.read_body
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
            @bearerToken = auth_bearer
        end

        ## list information on all files under the specified dir.
        def list_status(dir) 
            response = HTTParty.get("https://#{accountName}.azuredatalakestore.net" +
                    "/webhdfs/v1/#{dir}?op=LISTSTATUS", {
                    body: "grant_type=client_credentials&client_id=#{clientId}"+
                        "&client_secret=#{clientSecret}"+
                        "&resource=https%3A%2F%2Fmanagement.azure.com%2F",
                    headers: {
                        "Authorization" => "Bearer #{bearerToken}",
                        "Accept" => "*/*",
                        "Cache-Control" => 'no-cache',
                        "Host" => "#{accountName}.azuredatalakestore.net",
                        "Connection" => 'keep-alive',
                        "cache-control" => 'no-cache'
                    },
                    verify: true,
            })

            return JSON.parse response.read_body
        end

        ## list information on a single file.
        def get_file_status(file_path)
            response = HTTParty.get("https://#{accountName}.azuredatalakestore.net" +
                "/webhdfs/v1/#{file_path}?op=GETFILESTATUS", {
                body: "grant_type=client_credentials&client_id=#{clientId}"+
                    "&client_secret=#{clientSecret}"+
                    "&resource=https%3A%2F%2Fmanagement.azure.com%2F",
                headers: {
                    "Authorization" => "Bearer #{bearerToken}",
                    "Accept" => "*/*",
                    "Cache-Control" => 'no-cache',
                    "Host" => "#{accountName}.azuredatalakestore.net",
                    "Connection" => 'keep-alive',
                    "cache-control" => 'no-cache'
                },
                verify: true,
            })

            return JSON.parse response.read_body
        end

        # upload file to ADLS
        def create(filename, file, overwrite)
            # Create hierarchical directoy based on current time for
            # data lake organization.
            _time = Time.new
            _n_dir_path = "/#{_time.year}/#{_time.month}/#{_time.day}/"
            self.mkdir(_n_dir_path, 777)
            filename = "#{_n_dir_path}/#{filename}"

            # Execute request.
            response = HTTParty.put("https://#{accountName}.azuredatalakestore.net" + 
                "/webhdfs/v1/#{filename}?op=CREATE"+ 
                "&overwrite=#{overwrite}", {
                    body: file.read,
                    headers: {
                        "Authorization" => "Bearer #{bearerToken}",
                        "Accept" => "*/*",
                        "Cache-Control" => 'no-cache',
                        "Host" => "#{accountName}.azuredatalakestore.net",
                        "Connection" => 'keep-alive',
                        "cache-control" => 'no-cache',
                        "accept-encoding" => 'gzip, deflate',
                        "referer" => "https://#{accountName}.azuredatalakestore.net"+
                            "/webhdfs/v1/#{filename}?op=CREATE&overwrite=#{overwrite}",
                    },
                    verify: true
            })

            puts "File uploaded"
        end

        # Creates directories.
        def mkdir(path, permisions)
          response = HTTParty.put("https://#{accountName}.azuredatalakestore.net" + 
                                  "/webhdfs/v1/#{path}?op=MKDIRS" + 
                                  "&permission=#{permisions}", {
                    body: "grant_type=client_credentials&client_id=#{clientId}"+
                        "&client_secret=#{clientSecret}"+
                        "&resource=https%3A%2F%2Fmanagement.azure.com%2F",

                    headers: {
                        "Authorization" => "Bearer #{bearerToken}",
                        "Accept" => "*/*",
                        "Cache-Control" => 'no-cache',
                        "Host" => "#{accountName}.azuredatalakestore.net",
                        "Connection" => 'keep-alive',
                        "cache-control" => 'no-cache'
                    },
                    verify: true
          })

          return JSON.parse response.read_body
        end

        
    end
end



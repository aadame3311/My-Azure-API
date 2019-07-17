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


    ## ADLS Gen 1 service Api calls
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
                        "&resource=https%3A%2F%2Fmanagement.azure.com%2F" +
                        "&Content-Type=application/x-www-form-urlencoded"
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
                        "Authorization" => "Bearer " + @bearerToken,
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

        ## Displays the contents of the file path specified
        def open_file(file_path)
                response = HTTParty.get("https://#{accountName}.azuredatalakestore.net" +
                    "/webhdfs/v1/#{file_path}?op=OPEN", {
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
        
                return response.read_body
        end

                ## Displays content summary of the given file path
                def get_file_summary(file_path)
                    response = HTTParty.get("https://#{accountName}.azuredatalakestore.net" +
                        "/webhdfs/v1/#{file_path}?op=GETCONTENTSUMMARY", {
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
            
                    return  JSON.parse response.read_body
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
        def create(filename, file, overwrite, category, source, type)
            # Create hierarchical directoy based on current time for
            # data lake organization.
            _time = Time.new
            # Adds 0 before digit if less than ten (03, 04, 10).
            if _time.month < 10
                _m = "0#{_time.month}"
            else 
                _m = _time.month
            end
            if _time.day < 10
                _d = "0#{_time.day}"
            else
                _d = _time.day
            end

            _n_dir_path = "/landing_zone/#{category}/#{source}/#{type}"+"
                /year=#{_time.year}/month=#{_m}/day=#{_d}/"
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

    #Microsoft Graph Api calls for Active Directory operations
        class GRAPH 
            private
                attr_reader :tenantId, :clientId
                attr_reader :clientSecret
                attr_reader :bearerToken
        
                # Post request to login azure, returns bearer token 
                # to be used for authentication in Active Directory.
                def auth_bearer_aad
                    response = HTTParty.get("https://login.microsoftonline.com/#{tenantId}/oauth2/v2.0/token", {
                            body: "grant_type=client_credentials&client_id=#{clientId}"+
                                "&client_secret=#{clientSecret}"+
                                "&scope=https%3A%2F%2Fgraph.microsoft.com%2F.default"
                    })
        
                    parsed_json = JSON.parse response.read_body
                    return parsed_json["access_token"]
                end
        
            public
                def initialize()
                    @tenantId = MyAzure.get_tenant_id
                    @clientId = MyAzure.get_client_id
                    @clientSecret = MyAzure.get_client_secret
                    
                    # Generate bearer token.
                    @bearerToken = auth_bearer_aad
                 
                end
                
                ## Displays a list of users in the active directory
                def list_users()
                        response = HTTParty.get("https://graph.microsoft.com/v1.0/users", {  
                        headers: {
                                    "Authorization" => "Bearer #{bearerToken}",
                                    "Host" => 'graph.microsoft.com'  
                            }
                        })
                    return JSON.parse response.read_body
                end
        
                ## Displays the of the active directory
                def list_groups()
                        response = HTTParty.get("https://graph.microsoft.com/v1.0/groups", {  
                        headers: {
                                    "Authorization" => "Bearer #{bearerToken}",
                                    "Host" => 'graph.microsoft.com'   
                            }
                        })
                    return JSON.parse response.read_body
                end
        
                ## Displays the of the active directory
                def list_group_members(dept_id)
                        response = HTTParty.get("https://graph.microsoft.com/v1.0/groups/#{dept_id}/members", {  
                        headers: {
                                    "Authorization" => "Bearer #{bearerToken}",
                                    "Host" => 'graph.microsoft.com'   
                            }
                        })
                    return JSON.parse response.read_body
                end

                ## Adds a member to an active directory group given the department and user id
                ## Returns response code 204 no content if request is successful
                ## Returns response code 400 bad request if request failed or user already in group
                def add_group_member(dept_id, user_id)

                        user_create = {
                            "@odata.id" => "https://graph.microsoft.com/v1.0/directoryObjects/#{user_id}"
                        }

                        response = HTTParty.post("https://graph.microsoft.com/v1.0/groups/#{dept_id}/members/$ref", {  
                        
                        body: user_create.to_json,

                        headers: {
                                    "Authorization" => "Bearer #{bearerToken}",
                                    "Host" => 'graph.microsoft.com',
                                    "Content-Type" => 'application/json', 

                            }

                        })

                    case response.code
                        when 204
                          return response.code
                        when 400...600
                            return JSON.parse response.read_body
                    end
                
                end

                ## Removes a member from an active directory group given the department and user id
                ## Returns response code 204 no content if request is successful
                ## Returns response code 400 bad request if request failed and shows json response
                def remove_group_member(dept_id, user_id)

                    user_create = {
                        "@odata.id" => "https://graph.microsoft.com/v1.0/directoryObjects/#{user_id}"
                    }

                    response = HTTParty.delete("https://graph.microsoft.com/v1.0/groups/#{dept_id}/members/#{user_id}/$ref", {  

                    headers: {
                                "Authorization" => "Bearer #{bearerToken}",
                                "Host" => 'graph.microsoft.com',
                        }

                    })

                case response.code
                    when 204
                      return response.code
                    when 400...600
                        return JSON.parse response.read_body
                end
            
            end



                
            end

end



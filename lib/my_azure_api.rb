require 'net/http'
require 'json'
require 'httparty'
require 'time'

# Module to access azure services.
#   Has API calls for managing the file system on the data lake in the ADLS class.
#   Has API calls for managing users, groups, and permission in the GRAPH class.
module MyAzure
    include HTTParty

    private 
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

    public
    # User must first call this function with the proper credentinals before
    # making any API calls.
    # @param tenant [String] the tenant id for the Azure Data Lake
    # @param client [String] the client id for the Azure Data Lake
    # @param secret [String] the secret id for the Azure Data Lake
    # @param subscription [String] the subsription id that the Azure Data Lake is under
    # @return [JSON]
    def self.set_credentials(tenant, client, secret, subscription, resourceGroupName)
        @@tenantId = tenant
        @@clientId = client
        @@clientSecret = secret
        @@subscriptionId = subscription
        @@resourceGroupName = resourceGroupName
    end



    # ADLS Gen 1 service Api calls
    # This Class handles all the File System API for the Azure Data Lake
    class ADLS 
    private
        attr_reader :resource, :tenantId, :clientId
        attr_reader :clientSecret, :subscriptionId
        attr_reader :bearerToken, :accountName, :resourceGroupName
        
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
        # To initialize an instance of ADLS all the is need is the name of the
        # data lake.
        # @param accountName [String] name of Azure Data Lake
        def initialize(accountName)
            @accountName = accountName
            @resource = MyAzure.get_resource
            @tenantId = MyAzure.get_tenant_id
            @clientId = MyAzure.get_client_id
            @clientSecret = MyAzure.get_client_secret
            @subscriptionId = MyAzure.get_subscription_id
            @resourceGroupName = MyAzure.get_resourceGroupName
            
            # Generate bearer token.
            @bearerToken = auth_bearer
        end


        # Get information for specified Data Lake Storage Gen1 account
        # Returns response code 200 ok if successfully retrieved account information

        def get_account_info() 
            response = HTTParty.get("https://management.azure.com/subscriptions/#{subscriptionId}/resourceGroups/#{resourceGroupName}/providers/Microsoft.DataLakeStore/accounts/#{accountName}?api-version=2016-11-01", {
                    body: "grant_type=client_credentials&client_id=#{clientId}"+
                        "&client_secret=#{clientSecret}"+
                        "&resource=https%3A%2F%2Fmanagement.azure.com%2F",
                    headers: {
                        "Authorization" => "Bearer " + @bearerToken,
                        "Accept" => "*/*",
                        "Cache-Control" => 'no-cache',
                        #"Host" => "#{accountName}.azuredatalakestore.net",
                        "Connection" => 'keep-alive',
                        "cache-control" => 'no-cache'
                    },
                    verify: true,
            })

            return JSON.parse response.read_body
        end


        # list information on all files under the specified dir.
        # @param dir [String] the directory path in Azure Data Lake
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

        # Displays the contents of the file path specified
        # @param file_path [String] the path to the file in the Azure Data Lake
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

                # Displays content summary of the given file path 
                # @param file_path [String] the path to the file in the Azure Data Lake
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


        # list information on a single file.
        # @param file_path [String] the path to the file in the Azure Data Lake
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

        # creates new files in the Azure Data Lake
        # @param filename [String] the filename (without path)
        # @param file [File | StringIO | (Classes with the methods .read(size), .eof? )] the file to be uploaded to ADLS
        # @param overwrite [Boolean] if file exist overwrite it
        # @param category [String] the category of data the file contains
        # @param source [String] from which desitination is the data ariving
        # from
        # @param type [String] the type of data
        def create(filename, file, overwrite, category, source, type)
            # Create hierarchical directoy based on current time for
            # data lake organization.
            _time = Time.new<
            # Adds 0 before digit if less than ten (03, 04, 10).
            if _time.month < 10
                _m = "0#{_tim<e.month}"
            else
                _m = _time.month
            end
            if _time.day < 10
                _d = "0#{_time.day}"
            else
                _d = _time.day
            end

            _n_dir_path = "/landing_zone/#{category}/#{source}/#{type}"+
              "/year=#{_time.year}/month=#{_m}/day=#{_d}/"
            self.mkdir(_n_dir_path, 777)
            filename = "#{_n_dir_path}/#{filename}"

            # Execute request.
            response = HTTParty.put("https://#{accountName}.azuredatalakestore.net" +
                "/webhdfs/v1/#{filename}?op=CREATE"+
                "&overwrite=#{overwrite}", {
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

            chunk_size = 4 * 1024 * 1024
            count = 1
            until file.eof?
              puts "uploading file_chunk #{count}"
              file_chunk = file.read(chunk_size)
              append(filename, file_chunk)
              count += 1
            end

            puts "the response has a code of #{response.code}"
            puts "File uploaded"
        end

        # appends data to a file on the Azure Data Lake
        # @param file_path [String] the path to file in the Azure Data Lake
        # @param content [String] the data to append to the file
        def append(file_path, content)
            # Execute request.
            response = HTTParty.post("https://#{accountName}.azuredatalakestore.net" + 
                "/webhdfs/v1/#{file_path}?op=APPEND", {
                    body: content,
                    headers: {
                        "Authorization" => "Bearer #{bearerToken}",
                        "Accept" => "*/*",
                        "Cache-Control" => 'no-cache',
                        "Host" => "#{accountName}.azuredatalakestore.net",
                        "Connection" => 'keep-alive',
                        "cache-control" => 'no-cache',
                        "accept-encoding" => 'gzip, deflate',
                        "referer" => "https://#{accountName}.azuredatalakestore.net"+
                            "/webhdfs/v1/#{file_path}?op=APPEND",
                    },
                    verify: true
            })
            puts response.body
            puts "the response has a code of #{response.code}"
            puts "File uploaded"
        end
        
        # Creates directories.
        # @param path [String] the path of directory to create
        # @param permissions [Integer] work the same as *nix file system
        # permissions
        def mkdir(path, permissions)
          response = HTTParty.put("https://#{accountName}.azuredatalakestore.net" + 
                                  "/webhdfs/v1/#{path}?op=MKDIRS" + 
                                  "&permission=#{permissions}", {
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
                # creates an instance of the GRAPH API
                def initialize()
                    @tenantId = MyAzure.get_tenant_id
                    @clientId = MyAzure.get_client_id
                    @clientSecret = MyAzure.get_client_secret
                    
                    # Generate bearer token.
                    @bearerToken = auth_bearer_aad
                 
                end
                
                # Displays a list of users in the active directory
                def list_users()
                        response = HTTParty.get("https://graph.microsoft.com/v1.0/users", {  
                        headers: {
                                    "Authorization" => "Bearer #{bearerToken}",
                                    "Host" => 'graph.microsoft.com'  
                            }
                        })
                    return JSON.parse response.read_body
                end
        
                # Displays the of the active directory
                def list_groups()
                        response = HTTParty.get("https://graph.microsoft.com/v1.0/groups", {  
                        headers: {
                                    "Authorization" => "Bearer #{bearerToken}",
                                    "Host" => 'graph.microsoft.com'   
                            }
                        })
                    return JSON.parse response.read_body
                end
        
                # Displays the members in a Azure Active Directory group
                # param dept_id [String]  department id for the group
                def list_group_members(dept_id)
                        response = HTTParty.get("https://graph.microsoft.com/v1.0/groups/#{dept_id}/members", {  
                        headers: {
                                    "Authorization" => "Bearer #{bearerToken}",
                                    "Host" => 'graph.microsoft.com'   
                            }
                        })
                    return JSON.parse response.read_body
                end

                # Adds a member to an Azure Active Directory group
                # @param dept_id [String] the deparment id for the group
                # @param user_id [String] the id for the user to add
                # @return [Integer] the status code of the response 200 if successful or 400 if user already exist in group
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
                # @param dept_id [String] the deparment id for the group
                # @param user_id [String] the id for the user to add
                # @return [Integer] the status code of the response 200 if successful or 400 if user already exist in group
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



require_relative 'my_azure_api.rb'

# ADLS Gen 1 service Api calls
# This Class handles all the File System API for the Azure Data Lake
    class DATA_LAKE_STORAGE_GEN1
        include MyAzure

        private
            attr_reader :resource, :tenantId, :clientId
            attr_reader :clientSecret, :subscriptionId
            attr_reader :bearerToken, :accountName, :resourceGroupName
    
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
                @bearerToken = MyAzure.auth_bearer_management
            end
    
    
            # Get information for specified Data Lake Storage Gen1 account
            # Returns response code 200 ok if successfully retrieved account information
            def get_gen1_account_info(name) 
                response = HTTParty.get("https://management.azure.com/subscriptions/#{subscriptionId}/resourceGroups/#{resourceGroupName}/providers/Microsoft.DataLakeStore/accounts/#{name}?api-version=2016-11-01", {
    
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
    
    
            # Get information for all specified Data Lake Storage Gen1 accounts
            # Returns response code 200 ok if successfully retrieved information from all accounts
    
            def list_gen1_accounts() 
                response = HTTParty.get("https://management.azure.com/subscriptions/#{subscriptionId}/providers/Microsoft.DataLakeStore/accounts?api-version=2016-11-01", {
    
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
            
            # Creates the specified Data Lake Storage Gen1 account
            # Returns response code 200 ok if successfully created account
            def create_gen1_account(name) 
    
                account_create = {
                    "location"=> "centralus",
                    "tags"=> {
                        "test_key"=> "test_value"
                    },
                    "identity"=> {
                        "type"=> "SystemAssigned"
                    },
                    "properties"=> {
                        "encryptionState"=> "Enabled",
                        "encryptionConfig"=> {
                        "type"=> "ServiceManaged",
                        },
                        "firewallState"=> "Disabled",
                        "firewallRules"=> [
                      
                        ],
                        "trustedIdProviderState"=> "Disabled",
                        "trustedIdProviders"=> [
                       
                        ],
                    
                        "newTier"=> "Consumption",
                        "firewallAllowAzureIps"=> "Enabled"
                    }
                }
    
                response = HTTParty.put("https://management.azure.com/subscriptions/#{subscriptionId}/resourceGroups/#{resourceGroupName}/providers/Microsoft.DataLakeStore/accounts/#{name}?api-version=2016-11-01", {
    
                        body: account_create.to_json,
    
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
    
    
            # list information on all files under the specified dir.
            # @param dir [String] the directory path in Azure Data Lake
            def list_status(dir) 
                response = HTTParty.get("https://#{accountName}.azuredatalakestore.net" +
                        "/webhdfs/v1/#{dir}?op=LISTSTATUS", {
                        body: "grant_type=client_credentials&client_id=#{clientId}"+
                            "&client_secret=#{clientSecret}"+
                            "&resource=https%3A%2F%2Fmanagement.azure.com%2F",
                        headers: {
                            "Authorization" => "Bearer #{bearerToken}",
                            "Accept" => '*/*',
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
            # @return [Integer] status code 200 if sucessful
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
    
                
                return response.code
            end
    
            # appends data to a file on the Azure Data Lake
            # @param file_path [String] the path to file in the Azure Data Lake
            # @param content [String] the data to append to the file
            # @return [Integer] status code 200 if sucessful
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
    
                response.code
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
    
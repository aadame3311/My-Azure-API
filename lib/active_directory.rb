require_relative 'my_azure_api.rb'

#Microsoft Graph Api calls for Active Directory operations
class ACTIVE_DIRECTORY 
        include MyAzure

        private
            attr_reader :tenantId, :clientId
            attr_reader :clientSecret
            attr_reader :bearerToken
    
        public
            # Creates an instance of the GRAPH API
            def initialize()
                @tenantId = MyAzure.get_tenant_id
                @clientId = MyAzure.get_client_id
                @clientSecret = MyAzure.get_client_secret
                
                # Generate bearer token.
                @bearerToken = MyAzure.auth_bearer_graph
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
            # Param dept_id [String] department id for the group
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

            # Removes a member from an active directory group given the department and user id
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

end #End of Active Directory class
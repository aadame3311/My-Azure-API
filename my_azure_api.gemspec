Gem::Specification.new do |s|
  s.name        = 'my_azure_api'
  s.version     = '0.0.2'
  s.date        = '2019-07-29'
  s.summary     = "API for connecting to Microsoft Azure services"
  s.description = "Services supported will include Azure Data Lake storage Gen1, Data Factory, and SQL Warehouse"
  s.authors     = ["Data Duck Developers"]
  s.email       = 'rogueoneteam@utrgv.edu'
  s.files       = ["lib/my_azure_api.rb","lib/graph.rb", "lib/adls.rb"]
  s.homepage    =
    'https://github.com/aadame3311/My-Azure-API'
  s.license       = 'MIT'
  s.add_dependency 'httparty'
end

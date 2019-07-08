# My-Azure-API
Azure Ruby API built on top of the Azure Rest API. Main purpose is to portray a simplified way of doing operations on ADLS Gen1. Hoping to add support for more Azure services in the future.

### set credentials. must do before any api call.
```ruby
MyAzure.set_credentials(tenant_id, client_id, client_secret, subscription_id, application_id)
```
the above generates a bearer token that is used throughout all subsequent api calls.

# ADLS Gen1 API
### instantiate adls service.
```ruby
adls = MyAzure::ADLS.new("[adls account name]")
```

### List files in directory.
> https://#{accountName}.azuredatalakestore.net/webhdfs/v1/#{path}?op=LISTSTATUS

```
MyAzure::ADLS#list_files(path)
```

#### ADLS list file output json format.
```json
{
  "FileStatuses": {
    "FileStatus": [
      {
        "length": xxxxxxxx,
        "pathSuffix": "somefile.txt",
        "type": "FILE",
        "blockSize": xxxxxxxxxx,
        "accessTime": xxxxxxxxxxxx,
        "modificationTime": xxxxxxxxxxxxx,
        "replication": 1,
        "permission": "770",
        "owner": xxxxxxxxxxx,
        "group": xxxxxxxxxxx
      },
    ]
  }
}
```

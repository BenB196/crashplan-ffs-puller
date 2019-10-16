# crashplan-ffs-puller

A third party Golang application for pulling [Code42's](https://www.code42.com/) [Crashplan](https://www.crashplan.com/en-us/) [File Forensic Search (FFS)](https://support.code42.com/Administrator/Cloud/Administration_console_reference/Forensic_File_Search_reference_guide) logs from their API.

The goal of this application is to allow the user to pull FFS logs from Code42's FFS API, so that they can be used in other applications outside of Code42's environment. This application is designed to either output the logs to JSON files or directly into [Elasticsearch](https://www.elastic.co/). This application is also designed to enhance the data the FFS logs contains, currently it supports Geo Location enhancement through integration with [IP-API](http://ip-api.com/).

## Important Notes:

- This application, while working, is still considered under development, as it has not be thoroughly tested. There are likely bugs and random errors/panics which may occur. If they do, please report them so that they can be fixed.
- The slowest part of this application is the downloading of the logs. You should try to optimize your queries as much as possible.

Limitations:

Code42 Crashplan FFS API has limitations like most APIs, these limitations affect the functionality of the application as followed:

1. 120 Queries per minute, any additional queries will be dropped. (never actually bothered to test if/how this limit is actually enforced)
1. 200,000 results returned per query. This limitation is kind of annoying to handle as there is no easy way to handle it. The API does not support paging and the only way to figure out how many results there is for a query is to first query, count, then if over 200,000 results, break up the query into smaller time increments and perform multiple queries to get all of the results.
1. The application only supports the /v1/fileevent/export API endpoint currently. This has to do with how the highly limited functionality of the /v1/fileevent endpoint which isn't well documented.


## Install

### Build from Source

```
$ mkdir -p $GOPATH/src/github.com/BenB196/
$ cd $GOPATH/src/github.com/BenB196/
$ git clone https://github.com/BenB196/crashplan-ffs-puller.git
$ cd crashplan-ffs-puller
$ env GOOS=desired_os GOARCH=design_architecture go build -o /path/to/output/location #This command varies slightly based off of OS.
$ /path/to/output/location/crashplan-ffs-puller --config=/path/to/config.json
```

### Precompiled Binaries

These are found attached to each official release. Currently only Windows amd64 and Linux amd64 binaries will be released. As the application progresses, or more needs come up, more binaries will be added.

### Docker

This application can also be run in a Docker container as well.

Pull ```docker pull benb196/crashplan-ffs-puller```

This application is stateful, so it is recommended that a volume be mounted to the container to allow files to be saved between container rebuilds.

```
docker run -d\
 -v /path/to/local/storage:/crashplan-ffs-puller\
 -v /path/to/config/file.json:/etc/crashplan-ffs-puller/config.json\
 benb196/crashplan-ffs-puller
```

#### Docker Compose (3)

```
version: "3"
  services:
    crashplan-ffs-puller:
      image: benb196/crashplan-ffs-puller
      volumes:
        - /path/to/local/storage:/crashplan-ffs-puller
        - /path/to/config/file.json:/etc/crashplan-ffs-puller/config.json
```

Note: If you enable [Prometheus](https://prometheus.io/) integration, you need to expose/open the port that the Prometheus endpoint is configured to listen on (default: 8080)

## Configuration

An example configuration file can be found [here](docs/example_config.json)

Currently, only JSON formatted configuration files are accepted, in the future YAML support may be added.

```
{
  "authURI": "https://www.crashplan.com/c42api/v3/auth/jwt?useBody=true",                                                   #This is the URI which has the Code42 authentication endpoint.
  "ffsURI": "https://forensicsearch-default.prod.ffs.us2.code42.com/forensic-search/queryservice/api/v1/fileevent/export",  #This is the URI which exposes the FFS API. Note: Currently only supports the fileevent/export endpoint.
  "ffsQueries": [{                                                                                                          #This is an area of FFS Queries + additional information.
    "name": "example_query_1",                                                                                              #Query name, must be unique.
    "username": "example@example.com",                                                                                      #Username, must be an email address.
    "password": "<password>",                                                                                               #Password, if it contains double quotes they must be escaped.
    "interval": "5s",                                                                                                       #Query interval, how often the query should be executed, must be in a Golang duration format.
    "timeGap": "10s",                                                                                                       #Query time gap, the amount of time that should be scraped during each query execution, must be in a Golang duration format.
    "query": {                                                                                                              #The actual FFS Query to execute
      "groups": [
        {
          "filters": [
            {
              "operator": "IS",
              "term": "fileName",
              "value": "*"
            },
            {
              "operator": "ON_OR_AFTER",                                                                                    #Only one ON_OR_AFTER filter can exist per query.
              "term": "insertionTimestamp",                                                                                 #Supports either eventTimestamp or insertionTimestamp.
              "value": "2019-08-29T16:31:48.728Z"                                                                           #See below for explanation on this time values.
            },
            {
              "operator": "ON_OR_BEFORE",                                                                                   #Only one ON_OR_BEFORE filter can exist per query.
              "term": "insertionTimestamp",                                                                                 #Supports either eventTimestamp or insertionTimestamp.
              "value": ""                                                                                                   #See below for explanation on this time values.
            }
          ],
          "filterClause": "AND"
        }
      ]
    },
    "outputType": "elastic",                                                                                                #Output type, supports either file, elastic, logstash
    "outputLocation": "/path/to/output",                                                                                    #This is needed even if not using file output type, as there are stateful files which need to be written and stored.
    "ip-api": {                                                                                                             #IP-API Integration
      "enabled": true,                                                                                                      #Enable IP-API support? Default = false
      "url": "http://ip-api.com/",                                                                                          #URL to use for IP-API. Default = http://ip-api.com/
      "apiKey": "",                                                                                                         #API Key if you are using the pro version of IP-API
      "fields": "status,message,continent,continentCode,country,countryCode,region,regionName,city,district,zip,lat,lon,timezone,isp,org,as,asname,reverse,mobile,proxy",   #IP-API fields
      "lang": ""                                                                                                            #IP-API out language
    },
    "elasticsearch": {                                                                                                      #Elasticsearch output information. Only matters if output type = elastic
      "numberOfShards": 1,                                                                                                  #The number of shards the index should be created with
      "numberOfReplicas": 0,                                                                                                #The number of replicas the index should be created with
      "indexName": "crashplan",                                                                                             #The index name
      "indexTimeAppend": "2006-01-02",                                                                                      #If you want to append a time format to the index name do it here. Must match the Golang time format pattern (This example is yyyy-MM-dd). Default: 2006-01-02
      "indexTimeGen": "onOrBefore",                                                                                         #How to determine what time to use for the time stamp. Supports timeNow, onOrBefore, eventTimestamp, or insertionTimestamp. Default: timeNow
      "useCustomIndexPattern": false                                                                                        #This allows you to use a custom Elasticsearch Index Template instead of using the build in Elasticsearch Index Pattern provided by the application. Default: false
      "elasticUrl": "http://elasticsearch:9200",                                                                            #The elasticsearch URL
      "sniffing": false,                                                                                                    #This determines whether the application will automatically try update its elasticsearch node list
      "bestCompression": false,                                                                                             #This allows for indexes to be created with best_compression codec enabled
      "refreshInterval": 30,                                                                                                #This allows you to set the refresh interval (in seconds) of the index template. If empty it disables refresh interval
      "basicAuth": {                                                                                                        #If you are using basic auth with elasticsearch
        "user": "",
        "password": ""
      },
      "aliases": ["test1","test2"]                                                                                          #Any aliases you want the index to be created with.
    }
    "logstash": {                                                                                                           #Logstash output
      "logstashURL": "192.168.1.105:8080"                                                                                   #Address of logstash
    }
    "esStandardized": "",                                                                                                   #esStandardized This allows for the output to be formatted in standard Crashplan FFS (""), Semi Elastic Standard ("half"), or full Elastic Standard ("full")
    "validIpAddressesOnly": true                                                                                            #Setting this to true makes the private IP Addresses valid. By default Crashplan FFS provides invalid private IP addresses.
  },
    {
      "name": "example_query_2",                                                                                            #Second example FFS query, with extremely simple setup.
      "username": "example@example.com",
      "password": "<password>",
      "interval": "5s",
      "timeGap": "15s",
      "query": {
        "groups": [
          {
            "filters": [
              {
                "operator": "IS",
                "term": "fileName",
                "value": "*"
              },
              {
                "operator": "ON_OR_AFTER",
                "term": "insertionTimestamp",
                "value": "2019-08-29T16:31:48.727Z"
              },
              {
                "operator": "ON_OR_BEFORE",
                "term": "insertionTimestamp",
                "value": "2019-08-29T16:31:48.727Z"
              }
            ],
            "filterClause": "AND"
          }
        ]
      },
      "outputType": "file",
      "outputLocation": "/path/to/output"
    }],
  "prometheus": {                                                                                                           #Enable Prometheus Monitoring Support
    "enabled": true,                                                                                                        #Enable? Default = false
    "port": 8080                                                                                                            #Port for Prometheus /metric endpoint to listen on. Default = 8080 if enabled
  }
}
```

### Crashplan FFS Query Integration

In the above configuration there are some important notes to know about the FFS Queries.

1. Each query must contain a ON_OR_AFTER and ON_OR_BEFORE filter in order to properly work
1. Each query can only contain one ON_OR_AFTER and ON_OR_BEFORE filter. If either filter exists more than once, any of the filters past the first are ignored.
1. The values for ON_OR_AFTER and ON_OR_BEFORE are important (this is also why the application is stateful).
   1. If you leave ON_OR_AFTER empty, then the value of it will be set to time.Now (on the initial run only).
   1. If you set ON_OR_AFTER, then this is where the query will start off from (on the initial run only).
   1. If you leave ON_OR_BEFORE empty, then the query will run indefinitely (this is intended to be used to constantly pull new queries).
   1. If you set ON_OR_BEFORE, then once the query hits the value, the query will stop running entirely (this is intended really to pull a specific time interval of data).
1. Crashplan FFS provides invalid IPv6 addresses in the private IP address field. Setting validIpAddressesOnly to true corrects this issue.
   
Note: I have not tested out all possible queries in this application, if you come across a query which does not work, let me know and I will try to get it working.

### Elasticsearch Integration

If you are using the elastic output type there are a few important things to understand.

1. Currently this application does not support using a custom index mapping. If you would like to view the index map you can look [here](docs/index_mapping.json).
   1. There is the possibility that custom mapping support could be added if needed. In the mean time, if you would like to use this index with additional indexes, set an alias which can be used to join the indexes.
1. The number of shards to set depends on how much data you plan on generating for each index. The lazy rule of thumb is each shard can support 8GB-10GB of data.
   1. ex: You setup the index to have a daily naming schema, and the puller pulls approximately 25GB of data a day. You should set the number of shards to 3.
1. IndexTimeGen is a way of determining how to determine the time to use when appending to the end of an index name.
   1. timeNow, this will look at the current UTC and set the appended value based off of it (this is useful if you are only querying new data).
   1. onOrBefore, this will look at the onOrBefore time of the just completed query and set the appended value based off of it (this is useful if you are querying either old or new data, as it will spread the old data out over more indexes).
   1. eventTimestamp, this will look at the eventTimestamp of the event and set the index name based off of it
   1. insertTimestamp, this will look at the insertTimestamp of the event and set the index name based off of it.
1. If useCustomIndexPattern is set to true then you must set an Index Template up before proceeding. A basic index template can be found [here](docs/default_index_template.json).
   1. If useCustomIndexPattern is set to false the following Elasticsearch configuration settings are ignored:
      1. numberOfShards
      1. numberOfReplicas
      1. bestCompression
      1. refreshInterval
      1. aliases
1. If you use the esStandardized output (half or full), there is currently no build in template for this. Therefore you need to provide an index template on the elasticsearch side.
      
### Logstash Integration

Even though this is for Logstash integration, it uses a standard TCP socket output, so in theory this can be integrated with anything that accepts TCP data.

Logstash pipeline input:

```
input {
  tcp {
    port => 8080 #Set your port
  }
}
```

### IP-API Integration

The IP-API integration allows for the application to perform Geo IP lookups on the public IP Addresses which the FFS logs contain.

1. If you would like to know the supported fields or languages look [here](http://ip-api.com/docs/api:json)
2. Depending on how large your FFS environment is, this could add significant amount of processing time. In order to curb this, I have developed a proxy which allows for the lookups to be cached locally. You can find the proxy [here](https://github.com/BenB196/ip-api-proxy).

### Prometheus Integration

This application supports [Prometheus](https://prometheus.io/) metrics monitoring.

Besides the standard Golang metrics, the following application specific metrics have been added:

```
# HELP crashplan_ffs_puller_events_total The total number of processed FFS events
# TYPE crashplan_ffs_puller_events_total counter
crashplan_ffs_puller_events_total 0
# HELP crashplan_ffs_puller_in_progress_queries The current number of in progress queries
# TYPE crashplan_ffs_puller_in_progress_queries gauge
crashplan_ffs_puller_in_progress_queries 0
```

If you have any ideas for other metrics you feel may be useful, feel free to open an issue.

##TODOs (Maybe)

1. Add ability to use yaml/yml configuration files.
2. Add the ability to use the regular json FFS API endpoint (/fileevent).
3. Add some sort of file hash lookup that could provide threat intelligence (tried this with [OTX](https://www.alienvault.com/open-threat-exchange) before and failed pretty bad, may look at revisiting).
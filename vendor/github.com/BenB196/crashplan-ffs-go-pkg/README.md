# crashplan-ffs-go-pkg
A third-party Golang package for Code42's Crashplan Forensic File Search (FFS) API

The goal of this Golang package is to provide an easy to use/integrate package for Code42's Crashplan FFS API within the Golang environment. There are two main functions that can be used within the package:

1. GetAuthData
2. GetFileEvents

These functions allow for someone to query the Crashplan FFS API and get the results returned in a Golang struct which can then be used for other purposes.

## GetAuthData function
The GetAuthData is intended to get an API token for a user that will last for one (1) hour, which can then be used with the GetFileEvents function.

Arguments:
- uri - This is the URL which will provide the API token. (I believe it will always be: https://www.crashplan.com/c42api/v3/auth/jwt?useBody=true)
- username -  The username of the account that has permissions to access the FFS API. (This must be an email address according to the API)
- password -  The password of the account that is set as the username.

Returns:
- AuthData - Golang struct that contains the API token.
```
#AuthData struct structure
AuthData
    Data            AuthToken

AuthToken
    V3UserToken     string
```
- error - Any errors.

## GetFileEvents function

The GetFileEvents is intended to gather all events for a passed query and return them as a Golang struct slice.

Arguments:
- authData -  This is the Golang struct which is gotten from the GetAuthData function.
- ffsURI - This is the URL which actually hosts the FFS API. (See Code42 documentation for URI, default is https://forensicsearch-default.prod.ffs.us2.code42.com/forensic-search/queryservice/api/v1/)
- query - This is the properly formatted FFS Query struct which is what is actually executed against the Code42 Crashplan FFS API. (See documentation for how to properly format these queries.)
  - Example JSON query (Returns all events within a 5 second delta)

```
#Json Format
{
    "groups":[
       {
          "filters":[
             {
                "operator":"IS",
                "term":"fileName",
                "value":"*"
             },
             {
                "operator":"ON_OR_AFTER",
                "term":"insertionTimestamp",
                "value":"2019-08-18T20:31:48.728Z"
             },
             {
                "operator":"ON_OR_BEFORE",
                "term":"insertionTimestamp",
                "value":"2019-08-18T20:32:03.728Z"
             }
          ],
          "filterClause":"AND"
       }
    ],
    "groupClause":"AND",
    "pgNum":1,
    "pgSize":100,
    "srtDir":"asc",
    "srtKey":"insertionTimestamp"
}

#Query Struct format
Query
	Groups 		    []Group
	GroupClause     string      (optional)
	PgNum 		    int         (optional)
	PgSize 		    int         (optional)
	SrtDir 		    string      (optional)
	SrtKey 		    string      (optional)
}

Group
	Filters 	    []Filter
    FilterClause 	string      (optional)
}

Filter
	Operator 	    string
	Term 		    string
	Value 		    string
}
  ```
Returns:

- []FileEvent - Golang struct slice that contains all events returned from the jsonQuery string

```
#FileEvent struct structure
FileEvent
    EventId                     string	
    EventType                   string	
    EventTimestamp              time.Time       (potentially empty)
    InsertionTimestamp          time.Time       (potentially empty)
    FilePath                    string	
    FileName                    string	
    FileType                    string	
    FileCategory                string	
    FileSize                    int		
    FileOwner                   string          (potentially empty)
    Md5Checksum                 string	        (potentially empty)
    Sha256Checksum              string	        (potentially empty)
    CreatedTimestamp            time.Time       (potentially empty)
    ModifyTimestamp             time.Time       (potentially empty)
    DeviceUserName              string	
    DeviceUid                   string	
    UserUid                     string	
    OsHostName                  string	
    DomainName                  string	
    PublicIpAddress             string	        (potentially empty)
    PrivateIpAddresses          []string
    Actor                       string	        (potentially empty)
    DirectoryId                 []string        (potentially empty)
    Source                      string	
    Url                         string	        (potentially empty)
    Shared                      string	        (potentially empty)
    SharedWith                  []string        (potentially empty)
    SharingTypeAdded            []string        (potentially empty)
    CloudDriveId                string	        (potentially empty)
    DetectionSourceAlias        string	        (potentially empty)
    FileId                      string	        (potentially empty)
    Exposure                    []string        (potentially empty)
    ProcessOwner                string	        (potentially empty)
    ProcessName                 string	        (potentially empty)
    RemovableMediaVendor        string	        (potentially empty)
    RemovableMediaName          string	        (potentially empty)
    RemovableMediaSerialNumber  string	        (potentially empty)
    RemovableMediaCapacity      int             (potentially empty)
    RemovableMediaBusType       string	        (potentially empty)
    SyncDestination             string	        (potentially empty)
```

- error - Any errors.

Limitations:

Code42 Crashplan FFS API has limitations like most APIs, these limitations affect the GetFileEvents function:

1. 120 Queries per minute, any additional queries will be dropped. (never actually bothered to test if/how this limit is actually enforced)
2. 200,000 results returned per query. This limitation is kind of annoying to handle as there is no easy way to handle it. The API does not support paging and the only way to figure out how many results there is for a query is to first query, count, then if over 200,000 results, break up the query into smaller time increments and perform multiple queries to get all of the results.
3. The GetFileEvents function only supports the /v1/fileevent/export API endpoint currently. This has to do with how the highly limited functionality of the /v1/fileevent endpoint which isn't well documented (See below for a short rant about this once great endpoint).

## Code42 Documentation

Links for Code42 Documentation

- [Crashplan FFS API Documentation](https://support.code42.com/Administrator/Cloud/Monitoring_and_managing/Forensic_File_Search_API)

## /v1/fileevent Rant/Constructive Criticism

This endpoint used to be surprisingly useful. You could use paging to concurrently pull and process a time range and 10,000 events per page. You could determine how many results were in a time range and thus determine how many pages you would need to pull to process everything. It was all working great, so great actually I created an event exporter that would pull from the endpoint. The best thing about this endpoint though, is it is the only endpoint which exposes the data in JSON format, the best format for dealing with inconsistent amounts of fields and fields with arrays.

However, one day this endpoint was broken in an update. What I was told happened was the update broke the paging functionality, so that you could never pull the last page unless it was the same size as the query specified page size. I was told that this was some sort of rounding bug, and that a bug report was submitted. Shortly after this though, I was informed that the break was intentional, and that if I wanted to export large amounts of data, I would need to use the /export endpoint. The /export endpoint is nowhere close to as good as the /v1/fileevent endpoint used to be. First /export only exports in CSV, which is nowhere near as easy to use or reliable as JSON (Periodically this endpoint returns invalid CSV rows with improperly handled doubled quotes). Also, this endpoint lacks any sort of total results number, meaning that if you want to check if your query has gone over the allowed 200,000 events, you need to count all the events up yourself (a minor inconvenience, but still an inconvenience). This endpoint also lacks and sort of paging support, so if your query does contain more than 200,000 events, not only can you not just pull page 2, you need to shrink your query time scope, and re-query the exact same data you already pulled plus the additional missed data, because there is no way to determine where the first results actually end and where you would need to pick up from.

Enough criticism, and time for how the /v1/fileevent/export endpoint could be made better.
1. Add support for JSON output, CSV is terrible for anything other than reading in Excel
2. Add a field in the result which includes a total number of events in the result. (This one could be a bit tricky as I have no clue how you would do this in a CSV format.)
3. Add support for paging, I don't want to have to re-query the exact same data multiple times, until I come up with an interval that allows me to pull all of the data.

## TODOs

1. Figure out a way to build tests for these functions
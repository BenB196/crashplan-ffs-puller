# ip-api-go-pkg

A third party Golang package for integrating Golang projects with [IP-API's](http://ip-api.com/) API.

The goal of this Golang package is to provide an easy to use package for integrating IP-API's API into the Golang environment.

## Structs

There are two main structs within this package:

1. Query
2. Location

### Query struct

The query struct is design to be what is passed to the functions and eventually be executed against IP-API's API.

```
type Query struct {
	Queries	[]QueryIP   `json:"queries"`          Slice of QueryIPs. NOTE: If executing the single query function, only 1 QueryIP can be passed.
	Fields 	string      `json:"fields,omitempty"` This is a string of comma separated fields.
	Lang	string      `json:"lang,omitempty"`   This is a string of the language you wish to have returned.
}

type QueryIP struct {
	Query 	string      `json:"query"`            This is a string of either the IP address or DNS name you wish to query.
	Fields	string      `json:"fields,omitempty"` This is a string of comma separated fields. NOTE: Overwrites fields in Query struct.
	Lang    string      `json:"lang,omitempty"`   This is a string of the language you wish to have returned. NOTE: Overwrites lang in Query struct.
}
```

List of possible fields that can be passed: status, message, continent, continentCode, country, countryCode, region, regionName, city, district, zip, lat, lon, timezone, isp, org, as, asname, reverse, mobile, proxy, query [1](http://ip-api.com/docs/api:json)

List of possible languages that can be passed: en, de, es, pt-BR, fr, ja, zh-CN, ru [2](http://ip-api.com/docs/api:json)

### Location struct

The location struct is designed to take the return of the IP-API query and provide it in an easy to use struct.

```
type Location struct {
    Status          string      `json:"status,omitempty"`
    Message         string      `json:"message,omitempty"`
    Continent       string      `json:"continent,omitempty"`
    ContinentCode   string      `json:"continentCode,omitempty"`
    Country         string      `json:"country,omitempty"`
    CountryCode     string      `json:"countryCode,omitempty"`
    Region          string      `json:"region,omitempty"`
    RegionName      string      `json:"regionName,omitempty"`
    City            string      `json:"city,omitempty"`
    District        string      `json:"district,omitempty"`
    ZIP             string      `json:"zip,omitempty"`
    Lat             *float32    `json:"lat,omitempty"`
    Lon             *float32    `json:"lon,omitempty"`
    Timezone        string      `json:"timezone,omitempty"`
    Currency        string      `json:"currency,omitempty"`
    ISP             string      `json:"isp,omitempty"`
    Org             string      `json:"org,omitempty"`
    AS              string      `json:"as,omitempty"`
    ASName          string      `json:"asame,omitempty"`
    Reverse         string      `json:"reverse,omitempty"`
    Mobile          bool        `json:"mobile,omitempty"`
    Proxy           bool        `json:"proxy,omitempty"`
    Hosting         bool        `json:"hosting,omitempty"`
    Query           string      `json:"query,omitempty"`
}
```

## Functions

There are four (4) main functions within this package:

1. SingleQuery
2. BatchQuery
3. ValidateFields
4. ValidateLang

These functions allow someone to query IP-API's API within Golang and return the values as Golang structs to be used within other Golang applications.

### SingleQuery function

The SingleQuery function is designed to make a single request against the API.

Arguments:
- query - This is a Golang struct which when passed to the function will be reformatted into a proper query to be executed against the IP-API API.
- apiKey - This is for when you are using the pro version of IP-API and which to have the [increased functionality](https://members.ip-api.com/).
- baseURL - This is really only intended to be used if you are going through some sort of IP-API proxy. Otherwise, this can be left blank, and the URL will be determined by whether an API Key is provided or not.

Returns:
- Location - Golang struct that contains the results of the query.
- error - Any errors.

### BatchQuery function

The BatchQuery function is designed to take advantage of IP-API's [batch](http://ip-api.com/docs/api:batch) API. This is designed to allow for multiple queries to be executed at the same time to reduce overall query time.

Arguments:
- query - This is a Golang struct which when passed to the function will be reformatted into a proper query to be executed against the IP-API API.
- apiKey - This is for when you are using the pro version of IP-API and which to have the [increased functionality](https://members.ip-api.com/).
- baseURL - This is really only intended to be used if you are going through some sort of IP-API proxy. Otherwise, this can be left blank, and the URL will be determined by whether an API Key is provided or not.

Returns:
- []Location - Golang slice of location structs that contains the results of the query.
- error - Any errors.

An observation about the batch query. It sometime appears to return less data then the single query on the exact same query even when the same fields are passed. This is something that appears to be on the IP-API end, not the package end.

### ValidateFields function

The ValidateFields function is designed to validate that the fields which are being passed to the IP-API are valid.

Arguments:
- fields - This is a string which contains comma separated values of the fields. It will be checked against the AllowedAPIFields.

Returns:
- string - The same string which was passed to it in the fields argument.
- error - Any errors.

### ValidateLang function

The validateLang function is designed to validate the lang string which is one of the languages supported by IP-API.

Arguments:
- lang - the string which contains the desired language.

Returns:
- string - The same string which was passed to it in the lang argument.
- error - Any errors.

#Important

I currently have not tested the functionality of the Pro-stuff as I don't currently have access to it. If you encounter any issues with it, please let me know so that I can fix them.
package ip_api

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"
)

//URI for the free IP-API
const FreeAPIURI = "http://ip-api.com/"

//URI for the pro IP-API
const ProAPIURI = "https://pro.ip-api.com/"

type Location struct {
	Status 			string	`json:"status,omitempty"`
	Message			string	`json:"message,omitempty"`
	Continent		string	`json:"continent,omitempty"`
	ContinentCode	string	`json:"continentCode,omitempty"`
	Country			string	`json:"country,omitempty"`
	CountryCode		string	`json:"countryCode,omitempty"`
	Region			string	`json:"region,omitempty"`
	RegionName		string	`json:"regionName,omitempty"`
	City			string	`json:"city,omitempty"`
	District		string	`json:"district,omitempty"`
	ZIP				string	`json:"zip,omitempty"`
	Lat				float32	`json:"lat,omitempty"`
	Lon				float32	`json:"lon,omitempty"`
	Timezone		string	`json:"timezone,omitempty"`
	Currency		string	`json:"currency,omitempty"`
	ISP				string	`json:"isp,omitempty"`
	Org				string	`json:"org,omitempty"`
	AS				string	`json:"as,omitempty"`
	ASName			string	`json:"asame,omitempty"`
	Reverse			string	`json:"reverse,omitempty"`
	Mobile			bool	`json:"mobile,omitempty"`
	Proxy			bool	`json:"proxy,omitempty"`
	Query			string	`json:"query,omitempty"`
}

type Query struct {
	Queries	[]QueryIP 	`json:"queries"`
	Fields 	string		`json:"fields,omitempty"`
	Lang	string		`json:"lang,omitempty"`
}

type QueryIP struct {
	Query 	string 		`json:"query"`
	Fields	string		`json:"fields,omitempty"`
	Lang	string		`json:"lang,omitempty"`
}

//Execute a single query (queries field should only contain 1 value
func SingleQuery(query Query, apiKey string, baseURL string) (Location, error) {
	//Make sure that there is only 1 query value
	if len(query.Queries) != 1 {
		return Location{}, errors.New("error: only 1 query can be passed to single query api")
	}

	//Build URI
	uri := buildURI(query, "single",apiKey, baseURL)

	//Execute query
	req, err := http.NewRequest("GET",uri,nil)

	if err != nil {
		return Location{}, err
	}

	//Set request headers
	req.Header.Set("Accept","application/json")

	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		return Location{}, err
	}

	defer resp.Body.Close()

	//Check if invalid api key
	if resp.StatusCode == 403 {
		if strings.Contains(uri, "?key=") {
			return Location{}, errors.New("error: invalid api key")
		} else {
			return Location{}, errors.New("error: exceeded api calls per minute, you need to un-blacklist yourself")
		}
	}

	if resp.StatusCode != http.StatusOK {
		return Location{}, errors.New("error querying ip api: " + resp.Status + " " + strconv.Itoa(resp.StatusCode))
	}

	var location Location

	err = json.NewDecoder(resp.Body).Decode(&location)

	if err != nil {
		return Location{}, err
	}

	return location,nil
}

//Execute a batch query (queries field should contain 1 or more values
func BatchQuery(query Query, apiKey string, baseURL string) ([]Location, error) {
	//Make sure that there are 1 or more query values
	if len(query.Queries) < 1 {
		return nil, errors.New("error: no queries passed to batch query")
	}

	//Build URI
	uri := buildURI(query,"batch",apiKey, baseURL)

	//Build queries list
	queries, err := json.Marshal(query.Queries)

	if err != nil {
		return nil, err
	}

	log.Println(string(queries))

	//Execute Query
	req, err := http.NewRequest("POST",uri,bytes.NewReader(queries))

	if err != nil {
		return nil, err
	}

	//Set request headers
	req.Header.Set("Content-Type","application/json")

	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	//Check if invalid api key
	if resp.StatusCode == 403 {
		if strings.Contains(uri, "?key=") {
			return nil, errors.New("error: invalid api key")
		} else {
			return nil, errors.New("error: exceeded api calls per minute, you need to un-blacklist yourself")
		}
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("error querying ip api: " + resp.Status + " " + strconv.Itoa(resp.StatusCode))
	}

	var locations []Location

	err = json.NewDecoder(resp.Body).Decode(&locations)

	if err != nil {
		return nil, err
	}

	return locations,nil
}

func buildURI(query Query, queryType string, apiKey string, baseURL string) string {
	var baseURI string
	//Set base URI
	if baseURL != "" {
		baseURI = baseURL
	} else {
		switch apiKey {
		case "":
			baseURI = FreeAPIURI
		default:
			baseURI = ProAPIURI
		}
	}

	//Update base URI with query type
	switch queryType {
	case "single":
		baseURI = baseURI + "json/" + query.Queries[0].Query
	case "batch":
		baseURI = baseURI + "batch"
	}

	//Get fields list if fields len > 0
	var fieldsList string
	if len(query.Fields) > 0 {
		fieldsList = buildFieldList(query.Fields)
	}

	//Get lang string if lang != ""
	var lang string
	if query.Lang != "" {
		lang = buildLangString(query.Lang)
	}

	//Update base URI with api key if not ""
	switch apiKey {
	case "":
		if fieldsList != "" && lang != "" {
			baseURI = baseURI + "?" + fieldsList + "&" + lang
		} else if fieldsList != "" {
			baseURI = baseURI + "?" + fieldsList
		} else if lang != "" {
			baseURI = baseURI + "?" + lang
		}
	default:
		baseURI = baseURI + "?key=" + apiKey
		if fieldsList != "" && lang != "" {
			baseURI = baseURI + "&" + fieldsList + "&" + lang
		} else if fieldsList != "" {
			baseURI = baseURI + "&" + fieldsList
		} else if lang != "" {
			baseURI = baseURI + "&" + lang
		}
	}
	return baseURI
}

//Build fields string from slice
func buildFieldList(fields string) string {
	return "fields=" + fields
}

//Build lang string from lang value
func buildLangString(lang string) string {
	return "lang=" + lang
}

var AllowedAPIFields = []string{"status","message","continent","continentCode","country","countryCode","region","regionName","city","district","zip","lat","lon","timezone","isp","org","as","asname","reverse","mobile","proxy","query"}

var AllowedLangs = []string{"en","de","es","pt-BR","fr","ja","zh-CN","ru"}

/*
ValidateFields - validates the fields string to make sure it only has valid parameters
fields - string of comma separated values
*/
func ValidateFields(fields string) (string, error) {
	fieldsSlice := strings.Split(fields,",")

	for _, field := range fieldsSlice {
		if !contains(AllowedAPIFields, field) {
			return "", errors.New("error: illegal field provided: " + field)
		}
	}

	return fields, nil
}

/*
ValidateLang - validates the lang string to make sure it is a valid lang option
lang - string with lang value
*/
func ValidateLang(lang string) (string, error) {
	if !contains(AllowedLangs,lang) {
		return "", errors.New("error: illegal lang value provided: " + lang)
	}

	return lang, nil
}

/*
contains - checks a string slice to see if it contains a string
slice - string slice which you want to check
item - string which you want to see if exists in the string slice

returns
bool - true if slice contains string, else false
*/
func contains(slice []string, item string) bool {
	for _, value := range slice {
		if value == item {
			return true
		}
	}
	return false
}
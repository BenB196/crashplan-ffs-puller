package elastic

import (
	"crashplan-ffs-puller/config"
	"errors"
	"github.com/olivere/elastic/v7"
	"strconv"
	"strings"
	"time"
)

func BuildElasticClient(elasticsearch config.Elasticsearch) (*elastic.Client, error) {
	client := &elastic.Client{}
	var err error

	if elasticsearch.BasicAuth.User != "" {
		client, err = elastic.NewClient(
			elastic.SetURL(elasticsearch.ElasticURL),
			elastic.SetBasicAuth(elasticsearch.BasicAuth.User,elasticsearch.BasicAuth.Password),
			elastic.SetScheme(elasticsearch.Protocol))
	} else {
		client, err = elastic.NewClient(
			elastic.SetURL(elasticsearch.ElasticURL),
			elastic.SetScheme(elasticsearch.Protocol))
	}

	if err != nil {
		return nil, errors.New("error: failed to create elasticsearch client: " + err.Error())
	}

	return client, nil
}

func BuildIndexName(elasticsearch config.Elasticsearch) *string {
	if elasticsearch.IndexTimeAppend == "" {
		return &elasticsearch.IndexName
	}

	loc, _ := time.LoadLocation("UTC")
	currentTime := time.Now().In(loc).Format(elasticsearch.IndexTimeAppend)
	indexName := elasticsearch.IndexName + currentTime

	return &indexName
}

func BuildIndexPattern(elasticsearch config.Elasticsearch) *string {
	index := "{" +
		"  \"settings\": {" +
		"    \"number_of_shards\": " + strconv.Itoa(elasticsearch.NumberOfShards) + "," +
		"    \"number_of_replicas\": " + strconv.Itoa(elasticsearch.NumberOfReplicas) + "" +
		"  }," +
		"  \"mappings\": {" +
		"    \"fileEvent\": {" +
		"      \"_source\": {" +
		"        \"enabled\": true" +
		"      }," +
		"      \"properties\": {" +
		"        \"eventId\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"eventType\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"eventTimestamp\": {" +
		"          \"type\": \"date\"," +
		"          \"format\": \"yyyy-MM-dd'T'HH:mm:ss.SSSZ\"" +
		"        }," +
		"        \"insertionTimestamp\": {" +
		"          \"type\": \"date\"," +
		"          \"format\": \"yyyy-MM-dd'T'HH:mm:ss.SSSZ\"" +
		"        }," +
		"        \"filePath\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"fileName\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"fileType\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"fileCategory\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"fileSize\": {" +
		"          \"type\": \"long\"" +
		"        }," +
		"        \"fileOwner\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"md5Checksum\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"sha256Checksum\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"createdTimestamp\": {" +
		"          \"type\": \"date\"," +
		"          \"format\": \"yyyy-MM-dd'T'HH:mm:ss.SSSZ\"" +
		"        }," +
		"        \"modifyTimestamp\": {" +
		"          \"type\": \"date\"," +
		"          \"format\": \"yyyy-MM-dd'T'HH:mm:ss.SSSZ\"" +
		"        }," +
		"        \"deviceUsername\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"deviceUid\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"userUid\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"osHostname\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"domainName\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"publicIpAddress\": {" +
		"          \"type\": \"ip\"" +
		"        }," +
		"        \"privateIpAddresses\": {" +
		"          \"type\": \"ip\"" +
		"        }," +
		"        \"actor\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"directoryId\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"source\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"url\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"shared\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"sharedWith\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"sharingTypeAdded\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"cloudDriveId\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"detectionSourceAlias\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"fileId\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"exposure\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"processOwner\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"processName\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"removableMediaVendor\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"removableMediaName\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"removableMediaSerialNumber\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"removableMediaCapacity\": {" +
		"          \"type\": \"long\"" +
		"        }," +
		"        \"removableMediaBusType\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"syncDestination\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"status\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"message\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"continent\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"continentCode\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"country\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"countryCode\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"region\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"regionName\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"city\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"district\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"zip\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"lat\": {" +
		"          \"type\": \"float\"" +
		"        }," +
		"        \"lon\": {" +
		"          \"type\": \"float\"" +
		"        }," +
		"        \"timezone\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"currency\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"isp\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"org\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"as\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"asname\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"reverse\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"mobile\": {" +
		"          \"type\": \"boolean\"" +
		"        }," +
		"        \"proxy\": {" +
		"          \"type\": \"boolean\"" +
		"        }," +
		"        \"query\": {" +
		"          \"type\": \"keyword\"" +
		"        }," +
		"        \"geoPoint\": {" +
		"          \"type\": \"geo_point\"" +
		"        }" +
		"      }" +
		"    }" +
		"  }," +
		"  \"aliases\": {}" +
		"}"
	return &index
}

//Based of comments here: https://discuss.elastic.co/t/index-name-type-name-and-field-name-rules/133039
func ValidateIndexName(indexName string) error {
	if indexName == "" {
		return errors.New("error: index name cannot empty")
	}

	if strings.IndexAny(indexName,"ABCDEFGHIJKLMNOPQRSTUVWXYZ") > 0 {
		return errors.New("error: index name cannot contain any capitalized letters")
	}

	if strings.Contains(indexName,"\\") {
		return errors.New("error: index name cannot contain \"\\\"")
	}

	if strings.Contains(indexName,"/") {
		return errors.New("error: index name cannot contain \"/\"")
	}

	if strings.Contains(indexName,"*") {
		return errors.New("error: index name cannot contain \"*\"")
	}

	if strings.Contains(indexName,"?") {
		return errors.New("error: index name cannot contain \"?\"")
	}

	if strings.Contains(indexName,"\"") {
		return errors.New("error: index name cannot contain \"\"\"")
	}

	if strings.Contains(indexName,"<") {
		return errors.New("error: index name cannot contain \"<\"")
	}

	if strings.Contains(indexName,">") {
		return errors.New("error: index name cannot contain \">\"")
	}

	if strings.Contains(indexName,"|") {
		return errors.New("error: index name cannot contain \"|\"")
	}

	if strings.Contains(indexName," ") {
		return errors.New("error: index name cannot contain spaces")
	}

	if strings.HasPrefix(indexName,"_") {
		return errors.New("error: index name cannot start with \"_\"")
	}

	if strings.HasPrefix(indexName,"-") {
		return errors.New("error: index name cannot start with \"-\"")
	}

	if strings.HasPrefix(indexName,"+") {
		return errors.New("error: index name cannot start with \"+\"")
	}

	if indexName == "." {
		return errors.New("error: index name cannot be \".\"")
	}
	if indexName == ".." {
		return errors.New("error: index name cannot be \"..\"")
	}

	if len(indexName) > 255 {
		return errors.New("error: index name cannot be longer than 255 characters")
	}

	return nil
}
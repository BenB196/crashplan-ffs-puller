package elasticsearch

import (
	"errors"
	"github.com/BenB196/crashplan-ffs-puller/config"
	"github.com/olivere/elastic/v7"
	"strconv"
	"time"
)

func BuildElasticClient(elasticConfig config.Elasticsearch) (*elastic.Client, error) {
	client := &elastic.Client{}
	var err error

	if elasticConfig.BasicAuth.User != "" {
		client, err = elastic.NewClient(
			elastic.SetURL(elasticConfig.ElasticURL),
			elastic.SetBasicAuth(elasticConfig.BasicAuth.User,elasticConfig.BasicAuth.Password),
			elastic.SetSniff(elasticConfig.Sniffing))
	} else {
		client, err = elastic.NewClient(
			elastic.SetURL(elasticConfig.ElasticURL),
			elastic.SetSniff(elasticConfig.Sniffing))
	}

	if err != nil {
		return nil, errors.New("error: failed to create elasticsearch client: " + err.Error())
	}

	return client, nil
}

func BuildIndexName(elasticConfig config.Elasticsearch) string {
	if elasticConfig.IndexTimeAppend == "" {
		return elasticConfig.IndexName
	}

	loc, _ := time.LoadLocation("UTC")
	currentTime := time.Now().In(loc).Format(elasticConfig.IndexTimeAppend)
	indexName := elasticConfig.IndexName + "-" + currentTime

	return indexName
}

func BuildIndexNameWithTime(elasticConfig config.Elasticsearch, timeToAppend time.Time) string {
	if elasticConfig.IndexTimeAppend == "" {
		return elasticConfig.IndexName
	}

	indexName := elasticConfig.IndexName + "-" + timeToAppend.Format(elasticConfig.IndexTimeAppend)

	return indexName
}

func BuildIndexPattern(elasticConfig config.Elasticsearch) string {
	var codec string
	if elasticConfig.BestCompression {
		codec = "    \"codec\": \"best_compression\","
	} else {
		codec = ""
	}

	index := "{" +
		"  \"settings\": {" +
		codec +
		"    \"number_of_shards\": " + strconv.Itoa(elasticConfig.NumberOfShards) + "," +
		"    \"number_of_replicas\": " + strconv.Itoa(elasticConfig.NumberOfReplicas) + "" +
		"  }," +
		"  \"mappings\": {" +
		"    \"_source\": {" +
		"      \"enabled\": true" +
		"    }," +
		"    \"properties\": {" +
		"      \"eventId\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"eventType\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"eventTimestamp\": {" +
		"        \"type\": \"date\"" +
		"      }," +
		"      \"insertionTimestamp\": {" +
		"        \"type\": \"date\"" +
		"      }," +
		"      \"filePath\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"fileName\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"fileType\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"fileCategory\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"fileSize\": {" +
		"        \"type\": \"long\"" +
		"      }," +
		"      \"fileOwner\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"md5Checksum\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"sha256Checksum\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"createdTimestamp\": {" +
		"        \"type\": \"date\"" +
		"      }," +
		"      \"modifyTimestamp\": {" +
		"        \"type\": \"date\"" +
		"      }," +
		"      \"deviceUsername\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"deviceUid\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"userUid\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"osHostname\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"domainName\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"publicIpAddress\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"privateIpAddresses\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"actor\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"directoryId\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"source\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"url\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"shared\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"sharedWith\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"sharingTypeAdded\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"cloudDriveId\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"detectionSourceAlias\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"fileId\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"exposure\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"processOwner\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"processName\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"removableMediaVendor\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"removableMediaName\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"removableMediaSerialNumber\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"removableMediaCapacity\": {" +
		"        \"type\": \"long\"" +
		"      }," +
		"      \"removableMediaBusType\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"syncDestination\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"status\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"message\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"continent\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"continentCode\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"country\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"countryCode\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"region\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"regionName\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"city\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"district\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"zip\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"lat\": {" +
		"        \"type\": \"float\"" +
		"      }," +
		"      \"lon\": {" +
		"        \"type\": \"float\"" +
		"      }," +
		"      \"timezone\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"currency\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"isp\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"org\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"as\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"asname\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"reverse\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"mobile\": {" +
		"        \"type\": \"boolean\"" +
		"      }," +
		"      \"proxy\": {" +
		"        \"type\": \"boolean\"" +
		"      }," +
		"      \"query\": {" +
		"        \"type\": \"keyword\"" +
		"      }," +
		"      \"geoPoint\": {" +
		"        \"type\": \"geo_point\"" +
		"      }" +
		"    }" +
		"  }," +
		"  \"aliases\": {" + buildAliasString(elasticConfig.Aliases) + "}" +
		"}"
	return index
}

func buildAliasString(aliases []string) string {
	if len(aliases) <= 0 {
		return ""
	}

	var aliasString string

	for i, alias := range aliases {
		if i == 0 {
			if len(aliases) == 1 {
				aliasString = "\"" + alias + "\": {}"
			} else {
				aliasString = "\"" + alias + "\": {},"
			}
		} else if i == (len(aliases) -1) {
			aliasString = aliasString + "\"" + alias + "\": {}"
		} else {
			aliasString = aliasString + "\"" + alias + "\": {},"
		}
	}

	return aliasString
}

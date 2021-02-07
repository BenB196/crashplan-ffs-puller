package eventOutput

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"github.com/BenB196/crashplan-ffs-go-pkg"
	"github.com/BenB196/crashplan-ffs-puller/config"
	"github.com/BenB196/ip-api-go-pkg"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

type FFSEvent struct {
	ffs.JsonFileEvent
	*ip_api.Location `json:",omitempty"`
	GeoLocation      *Location `json:"geoPoint,omitempty"`
}

type Code42 struct {
	Event                   *Code42Event          `json:"event,omitempty"`
	InsertionTimestamp      *time.Time            `json:"insertion_timestamp,omitempty"`
	File                    *Code42File           `json:"file,omitempty"`
	Device                  *Code42Device         `json:"device,omitempty"`
	OsHostName              string                `json:"os_host_name,omitempty"`
	DomainName              string                `json:"domain_name,omitempty"`
	PublicIpAddress         string                `json:"public_ip_address,omitempty"`
	PrivateIpAddresses      []string              `json:"private_ip_addresses,omitempty"`
	Actor                   string                `json:"actor,omitempty"`
	DirectoryId             []string              `json:"directory_id,omitempty"`
	Source                  string                `json:"source,omitempty"`
	Url                     *URL                  `json:"url,omitempty"`
	Shared                  string                `json:"shared,omitempty"`
	SharedWith              []Code42SharedWith    `json:"shared_with,omitempty"`
	SharingTypeAdded        []string              `json:"sharing_type_added,omitempty"`
	CloudDriveId            string                `json:"cloud_drive_id,omitempty"`
	DetectionSourceAlias    string                `json:"detection_source_alias,omitempty"`
	Exposure                []string              `json:"exposure,omitempty"`
	Process                 *Code42Process        `json:"process,omitempty"`
	RemovableMedia          *Code42RemovableMedia `json:"removable_media,omitempty"`
	SyncDestination         string                `json:"sync_destination,omitempty"`
	SyncDestinationUsername []string              `json:"sync_destination_username,omitempty"`
	EmailDlp                *Code42EmailDlp       `json:"email_dlp,omitempty"`
	OutsideActiveHours      *bool                 `json:"outside_active_hours,omitempty"`
	Print                   *Code42Print          `json:"print,omitempty"`
	RemoteActivity          string                `json:"remote_activity,omitempty"`
	Trusted                 *bool                 `json:"trusted,omitempty"`
	OperatingSystemUser     string                `json:"operating_system_user,omitempty"`
	Destination             *Code42Destination    `json:"destination,omitempty"`
	Tabs                    []Code42TabTab        `json:"tabs,omitempty"`
}

type Code42TabTab struct {
	Title string `json:"title,omitempty"`
	Url   *URL   `json:"url,omitempty"`
}

type Code42SharedWith struct {
	CloudUsername *string `json:"cloud_username,omitempty"`
}

type Code42Event struct {
	Id        string     `json:"id,omitempty"`
	Type      string     `json:"type,omitempty"`
	Timestamp *time.Time `json:"timestamp,omitempty"`
}

type Code42File struct {
	Path                string     `json:"path,omitempty"`
	Name                string     `json:"name,omitempty"`
	Type                string     `json:"type,omitempty"`
	Category            string     `json:"category,omitempty"`
	MimeTypeByBytes     string     `json:"mime_type_by_bytes,omitempty"`
	MimeTypeByExtension string     `json:"mime_type_by_extension,omitempty"`
	Size                *int64     `json:"size,omitempty"`
	Owner               string     `json:"owner,omitempty"`
	Hash                *Hash      `json:"hash,omitempty"`
	CreateTimestamp     *time.Time `json:"create_timestamp,omitempty"`
	ModifyTimestamp     *time.Time `json:"modify_timestamp,omitempty"`
	Id                  string     `json:"id,omitempty"`
	MimeTypeMismatch    *bool      `json:"mime_type_mismatch,omitempty"`
	CategoryByBytes     string     `json:"category_by_bytes,omitempty"`
	CategoryByExtension string     `json:"category_by_extension,omitempty"`
}

type Code42Device struct {
	Username string `json:"username,omitempty"`
	Uid      string `json:"uid,omitempty"`
}

type Code42Tab struct {
	WindowTitle string `json:"window_title,omitempty"`
	Url         *URL   `json:"url,omitempty"`
}

type Code42RemovableMedia struct {
	Vendor       string   `json:"vendor,omitempty"`
	Name         string   `json:"name,omitempty"`
	SerialNumber string   `json:"serial_number,omitempty"`
	Capacity     *int64   `json:"capacity,omitempty"`
	BusType      string   `json:"bus_type,omitempty"`
	MediaName    string   `json:"media_name,omitempty"`
	VolumeName   []string `json:"volume_name,omitempty"`
	PartitionId  []string `json:"partition_id,omitempty"`
}

type Code42EmailDlp struct {
	PolicyNames []string `json:"policy_names,omitempty"`
	Subject     string   `json:"subject,omitempty"`
	Sender      string   `json:"sender,omitempty"`
	From        string   `json:"from,omitempty"`
	Recipients  []string `json:"recipients,omitempty"`
}

type Code42Print struct {
	JobName     string `json:"job_name,omitempty"`
	PrinterName string `json:"name,omitempty"`
}

type Code42Destination struct {
	Category string `json:"category,omitempty"`
	Name     string `json:"name,omitempty"`
}

type ElasticFileEvent struct {
	Event     *Event     `json:"event,omitempty"`
	Timestamp *time.Time `json:"@timestamp,omitempty"`
	File      *File      `json:"file,omitempty"`
	Host      *Host      `json:"host,omitempty"`
	Code42    *Code42    `json:"code_42,omitempty"`
}

type Event struct {
	Action   string     `json:"action,omitempty"`
	Category string     `json:"category,omitempty"`
	Created  *time.Time `json:"created,omitempty"`
	Dataset  string     `json:"dataset,omitempty"`
	Id       string     `json:"id,omitempty"`
	Ingested *time.Time `json:"ingested,omitempty"`
	Kind     string     `json:"kind,omitempty"`
	Module   string     `json:"module,omitempty"`
	Outcome  string     `json:"outcome,omitempty"`
	Provider string     `json:"provider,omitempty"`
	Type     string     `json:"type,omitempty"`
}

type Hash struct {
	Md5    string `json:"md5,omitempty"`
	Sha256 string `json:"sha256,omitempty"`
}

type URL struct {
	Full             string `json:"full,omitempty"`
	Domain           string `json:"domain,omitempty"`
	Extension        string `json:"extension,omitempty"`
	Fragment         string `json:"fragment,omitempty"`
	Path             string `json:"path,omitempty"`
	Port             *int   `json:"port,omitempty"`
	Query            string `json:"query,omitempty"`
	Scheme           string `json:"scheme,omitempty"`
	Username         string `json:"username,omitempty"`
	Password         string `json:"password,omitempty"`
	RegisteredDomain string `json:"registered_domain,omitempty"`
	TopLevelDomain   string `json:"top_level_domain,omitempty"`
}

type File struct {
	Created   *time.Time `json:"created,omitempty"`
	Directory []string   `json:"directory,omitempty"`
	Extension string     `json:"extension,omitempty"`
	MimeType  []string   `json:"mime_type,omitempty"`
	Mtime     *time.Time `json:"mtime,omitempty"`
	Name      string     `json:"name,omitempty"`
	Owner     string     `json:"owner,omitempty"`
	Path      string     `json:"path,omitempty"`
	Size      *int64     `json:"size,omitempty"`
	Type      string     `json:"type,omitempty"`
	Hash      *Hash      `json:"hash,omitempty"`
}

type User struct {
	Email  string `json:"email,omitempty"`
	Id     string `json:"id,omitempty"`
	Name   string `json:"name,omitempty"`
	Domain string `json:"domain,omitempty"`
}

type Host struct {
	Id       string   `json:"id,omitempty"`
	Name     string   `json:"name,omitempty"`
	Hostname string   `json:"hostname,omitempty"`
	User     *User    `json:"user,omitempty"`
	IP       []string `json:"ip,omitempty"`
	Geo      *Geo     `json:"geo,omitempty"`
}

type Code42Process struct {
	Owner string `json:"owner,omitempty"`
	Name  string `json:"name,omitempty"`
}

type Geo struct {
	Status        string    `json:"status,omitempty"`
	Message       string    `json:"message,omitempty"`
	Continent     string    `json:"continent_name,omitempty"`
	ContinentCode string    `json:"continent_iso_code,omitempty"`
	Country       string    `json:"country_name,omitempty"`
	CountryCode   string    `json:"country_iso_code,omitempty"`
	Region        string    `json:"region_iso_code,omitempty"`
	RegionName    string    `json:"region_name,omitempty"`
	City          string    `json:"city_name,omitempty"`
	District      string    `json:"district,omitempty"`
	ZIP           string    `json:"postal_code,omitempty"`
	Lat           *float32  `json:"lat,omitempty"`
	Lon           *float32  `json:"lon,omitempty"`
	Timezone      string    `json:"timezone,omitempty"`
	Currency      string    `json:"currency,omitempty"`
	ISP           string    `json:"isp,omitempty"`
	Org           string    `json:"org,omitempty"`
	AS            string    `json:"as,omitempty"`
	ASName        string    `json:"as_name,omitempty"`
	Reverse       string    `json:"reverse,omitempty"`
	Mobile        *bool     `json:"mobile,omitempty"`
	Proxy         *bool     `json:"proxy,omitempty"`
	Hosting       *bool     `json:"hosting,omitempty"`
	Query         string    `json:"query,omitempty"`
	Location      *Location `json:"location,omitempty"`
}

type Location struct {
	Lat *float32 `json:"lat,omitempty"`
	Lon *float32 `json:"lon,omitempty"`
}

func WriteEvents(ffsEvents interface{}, query config.FFSQuery) error {
	//Error if ffsEvents is nil, this should not be called if there are no ffsEvents
	if ffsEvents == nil {
		return errors.New("error: ffsEvents is nil")
	}

	//Check if outputLocation is provided
	if query.OutputLocation == "" {
		return errors.New("error: no output location provided")
	}

	//Generate filename
	fileName, err := generateEventFileName(query)

	//handle errs
	if err != nil {
		return err
	}

	//Create output file
	file, err := os.Create(fileName)

	defer func() {
		if err := file.Close(); err != nil {
			panic(errors.New("error: closing file: " + fileName + " " + err.Error()))
		}
	}()

	if err != nil {
		return errors.New("error: creating file: " + fileName + " " + err.Error())
	}

	//Create buffer writer
	w := bufio.NewWriter(file)

	//Build ffsEvents string
	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	err = encoder.Encode(ffsEvents)

	if err != nil {
		return err
	}

	//Split json array into individual json objects, makes every faster down the line
	ffsEventsString := strings.Replace(strings.Replace(strings.ReplaceAll(string(buffer.Bytes()), "},{", "}\n{"), "[{", "{", 1), "}]", "}", 1)

	//Write events to file
	if ffsEventsString != "" {
		_, err := w.WriteString(ffsEventsString)

		if err != nil {
			return errors.New("error: writing events to file: " + fileName + " " + err.Error())
		}
	}

	err = w.Flush()

	if err != nil {
		return errors.New("error: flushing file: " + fileName + " " + err.Error())
	}

	err = file.Sync()

	if err != nil {
		return errors.New("error: syncing file: " + fileName + " " + err.Error())
	}

	return nil
}

func generateEventFileName(query config.FFSQuery) (string, error) {
	//Check if query.Groups is not 0, will need to generate filename
	if len(query.Query.Groups) == 0 {
		return "", errors.New("error: no groups provided")
	}

	//Create filename var
	var fileName string

	for _, groups := range query.Query.Groups {
		//Check if groups.Filters is not 0, will need to generate filename
		if len(groups.Filters) == 0 {
			return "", errors.New("error: no filters provided")
		}
		for _, filters := range groups.Filters {
			if filters.Operator == "ON_OR_AFTER" {
				//Get value in golang time
				onOrAfter, err := time.Parse(time.RFC3339Nano, filters.Value)
				if err != nil {
					return "", errors.New("error getting on or after value for file name: " + err.Error())
				}
				fileName = fileName + "A" + onOrAfter.Format("2006.01.02.15.04.05.000")
			} else if filters.Operator == "ON_OR_BEFORE" {
				//Get value in golang time
				onOrBefore, err := time.Parse(time.RFC3339Nano, filters.Value)
				if err != nil {
					return "", errors.New("error getting on or before value for file name: " + err.Error())
				}
				fileName = fileName + "B" + onOrBefore.Format("2006.01.02.15.04.05.000")
			}
		}
	}

	fileName = query.Name + fileName

	//Validate that a filename was generated
	if fileName == "" {
		return "", errors.New("failed to generate file name properly")
	} else if len(fileName) > 248 {
		//truncate filename if longer than 248 characters
		fileName = fileName[0:248]
	}

	//Add file extension
	//TODO add support for other outputs?
	fileName = query.OutputLocation + fileName + ".json"

	return fileName, nil
}

//In progress query struct
type InProgressQuery struct {
	OnOrAfter  time.Time
	OnOrBefore time.Time
}

//In progress query struct using strings
type InProgressQueryString struct {
	OnOrAfter  string
	OnOrBefore string
}

func WriteInProgressQueries(query config.FFSQuery, inProgressQueries []InProgressQuery) error {
	fileName := query.OutputLocation + query.Name + "inProgressQueries.json"
	file, err := os.Create(fileName)

	if err != nil {
		return errors.New("error: creating file for in progress queries for ffs query: " + query.Name + " : " + err.Error())
	}

	defer func() {
		if err := file.Close(); err != nil {
			panic(errors.New("error: closing file: " + fileName + " " + err.Error()))
		}
	}()

	w := bufio.NewWriter(file)

	inProgressQueriesBytes, err := json.Marshal(inProgressQueries)

	if err != nil {
		return errors.New("error: marshaling in progress queries for ffs query: " + query.Name)
	}

	_, err = w.Write(inProgressQueriesBytes)

	if err != nil {
		return errors.New("error: writing in progress queries to file: " + fileName + " " + err.Error())
	}

	err = w.Flush()

	if err != nil {
		return errors.New("error: flushing file: " + fileName + " " + err.Error())
	}

	err = file.Sync()

	if err != nil {
		return errors.New("error: syncing file: " + fileName + " " + err.Error())
	}

	return nil
}

func ReadInProgressQueries(query config.FFSQuery) ([]InProgressQuery, error) {
	fileName := query.OutputLocation + query.Name + "inProgressQueries.json"
	inProgressQueryData, err := ioutil.ReadFile(fileName)
	if err != nil {
		if strings.Contains(err.Error(), "The system cannot find the file specified") || strings.Contains(err.Error(), "no such file or directory") {
			err = WriteInProgressQueries(query, nil)
			return nil, err
		}
		return nil, err
	}

	fileStats, err := os.Stat(fileName)

	if err != nil {
		return nil, errors.New("error: unable to check if in progress queries file is empty: " + err.Error())
	}

	if fileStats.Size() == 0 {
		return nil, nil
	}

	var inProgressQueryStrings []InProgressQueryString

	err = json.Unmarshal(inProgressQueryData, &inProgressQueryStrings)

	if err != nil {
		return nil, errors.New("error: parsing in progress queries from: " + fileName + " " + err.Error())
	}

	var inProgressQueries []InProgressQuery

	for _, inProgressQueryString := range inProgressQueryStrings {
		onOrAfter, err := time.Parse(time.RFC3339Nano, inProgressQueryString.OnOrAfter)
		if err != nil {
			return nil, errors.New("error: parsing on or after from in progress queries in file: " + fileName)
		}

		onOrBefore, err := time.Parse(time.RFC3339Nano, inProgressQueryString.OnOrBefore)
		if err != nil {
			return nil, errors.New("error: parsing on or before from in progress queries in file: " + fileName)
		}

		inProgressQueries = append(inProgressQueries, InProgressQuery{
			OnOrAfter:  onOrAfter,
			OnOrBefore: onOrBefore,
		})
	}
	return inProgressQueries, nil
}

func WriteLastCompletedQuery(query config.FFSQuery, lastCompletedQuery InProgressQuery) error {
	fileName := query.OutputLocation + query.Name + "lastCompletedQuery.json"
	file, err := os.Create(fileName)

	if err != nil {
		return errors.New("error: creating file for last completed ffs query: " + query.Name + " : " + err.Error())
	}

	defer func() {
		if err := file.Close(); err != nil {
			panic(errors.New("error: closing file: " + fileName + " " + err.Error()))
		}
	}()

	w := bufio.NewWriter(file)

	lastCompletedQueryBytes, err := json.Marshal(lastCompletedQuery)

	if err != nil {
		return errors.New("error: marshaling last completed query for ffs query: " + query.Name)
	}

	_, err = w.Write(lastCompletedQueryBytes)

	if err != nil {
		return errors.New("error: writing last completed query to file: " + fileName + " " + err.Error())
	}

	err = w.Flush()

	if err != nil {
		return errors.New("error: flushing file: " + fileName + " " + err.Error())
	}

	err = file.Sync()

	if err != nil {
		return errors.New("error: syncing file: " + fileName + " " + err.Error())
	}

	return nil
}

func ReadLastCompletedQuery(query config.FFSQuery) (InProgressQuery, error) {
	fileName := query.OutputLocation + query.Name + "lastCompletedQuery.json"
	lastCompletedQueryData, err := ioutil.ReadFile(fileName)
	if err != nil {
		if strings.Contains(err.Error(), "The system cannot find the file specified") || strings.Contains(err.Error(), "no such file or directory") {
			err = WriteLastCompletedQuery(query, InProgressQuery{})
			return InProgressQuery{}, err
		}
		return InProgressQuery{}, err
	}

	fileStats, err := os.Stat(fileName)

	if err != nil {
		return InProgressQuery{}, errors.New("error: unable to check if in progress queries file is empty: " + err.Error())
	}

	if fileStats.Size() == 0 {
		return InProgressQuery{}, nil
	}

	var lastCompletedQueryString InProgressQueryString

	err = json.Unmarshal(lastCompletedQueryData, &lastCompletedQueryString)

	if err != nil {
		return InProgressQuery{}, errors.New("error: parsing last completed query from: " + fileName + " " + err.Error())
	}

	onOrAfter, err := time.Parse(time.RFC3339Nano, lastCompletedQueryString.OnOrAfter)
	if err != nil {
		return InProgressQuery{}, errors.New("error: parsing on or after from last completed query in file: " + fileName)
	}

	onOrBefore, err := time.Parse(time.RFC3339Nano, lastCompletedQueryString.OnOrBefore)
	if err != nil {
		return InProgressQuery{}, errors.New("error: parsing on or before from last completed in file: " + fileName)
	}

	return InProgressQuery{
		OnOrAfter:  onOrAfter,
		OnOrBefore: onOrBefore,
	}, nil
}

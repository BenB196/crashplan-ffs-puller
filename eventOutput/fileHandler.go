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
	ffs.FileEvent
	*ip_api.Location `json:",omitempty"`
	GeoLocation      *Location `json:"geoPoint,omitempty"`
}

type SemiElasticFFSEvent struct {
	FileEvent SemiElasticFileEvent `json:"file_event"`
	Geo       *Geo                 `json:"geo,omitempty"`
}

type SemiElasticFileEvent struct {
	EventId                     string     `json:"event_id"`
	EventType                   string     `json:"event_type"`
	EventTimestamp              *time.Time `json:"event_timestamp,omitempty"`
	InsertionTimestamp          *time.Time `json:"insertion_timestamp,omitempty"`
	FilePath                    string     `json:"file_path,omitempty"`
	FileName                    string     `json:"file_name"`
	FileType                    string     `json:"file_type,omitempty"`
	FileCategory                string     `json:"file_category,omitempty"`
	IdentifiedExtensionCategory string     `json:"identified_extension_category,omitempty"`
	CurrentExtensionCategory    string     `json:"current_extension_category,omitempty"`
	FileSize                    *int       `json:"file_size"`
	FileOwner                   []string   `json:"file_owner,omitempty"` //Array of owners
	Md5Checksum                 string     `json:"md5_checksum,omitempty"`
	Sha256Checksum              string     `json:"sha256_checksum,omitempty"`
	CreatedTimestamp            *time.Time `json:"created_timestamp,omitempty"`
	ModifyTimestamp             *time.Time `json:"modify_timestamp,omitempty"`
	DeviceUsername              string     `json:"device_username,omitempty"`
	DeviceUid                   string     `json:"device_uid,omitempty"`
	UserUid                     string     `json:"user_uid,omitempty"`
	OsHostname                  string     `json:"os_hostname,omitempty"`
	DomainName                  string     `json:"domain_name,omitempty"`
	PublicIpAddress             string     `json:"public_ip_address,omitempty"`
	PrivateIpAddresses          []string   `json:"private_ip_addresses,omitempty"` //Array of IP address strings
	Actor                       string     `json:"actor,omitempty"`
	DirectoryId                 []string   `json:"directory_id,omitempty"` //An array of something, I am not sure
	Source                      string     `json:"source,omitempty"`
	Url                         string     `json:"url,omitempty"`
	Shared                      *bool      `json:"shared,omitempty"`
	SharedWith                  []string   `json:"shared_with,omitempty"` //An array of strings (Mainly Email Addresses)
	SharingTypeAdded            []string   `json:"sharing_type_added,omitempty"`
	CloudDriveId                string     `json:"cloud_drive_id,omitempty"`
	DetectionSourceAlias        string     `json:"detection_source_alias,omitempty"`
	FileId                      string     `json:"file_id,omitempty"`
	Exposure                    []string   `json:"exposure,omitempty"`
	ProcessOwner                string     `json:"process_owner,omitempty"`
	ProcessName                 string     `json:"process_name,omitempty"`
	TabWindowTitle              string     `json:"tab_window_title,omitempty"`
	TabUrl                      string     `json:"tab_url,omitempty"`
	RemovableMediaVendor        string     `json:"removable_media_vendor,omitempty"`
	RemovableMediaName          string     `json:"removable_media_name,omitempty"`
	RemovableMediaSerialNumber  string     `json:"removable_media_serial_number,omitempty"`
	RemovableMediaCapacity      *int       `json:"removable_media_capacity,omitempty"`
	RemovableMediaBusType       string     `json:"removable_media_bus_type,omitempty"`
	RemovableMediaMediaName     string     `json:"removable_media_media_name,omitempty"`
	RemovableMediaVolumeName    string     `json:"removable_media_volume_name,omitempty"`
	RemovableMediaPartitionId   string     `json:"removable_media_partition_id,omitempty"`
	SyncDestination             string     `json:"sync_destination,omitempty"`
	SyncDestinationUsername     string     `json:"sync_destination_username,omitempty"`
	EmailDLPPolicyNames         []string   `json:"email_dlp_policy_names,omitempty"`
	EmailDLPSubject             string     `json:"email_dlp_subject,omitempty"`
	EmailDLPSender              string     `json:"email_dlp_sender,omitempty"`
	EmailDLPFrom                string     `json:"email_dlp_from,omitempty"`
	EmailDLPRecipients          []string   `json:"email_dlp_recipients,omitempty"`
	OutsideActiveHours          *bool      `json:"outside_active_hours,omitempty"`
	IdentifiedExtensionMIMEType string     `json:"identified_extension_mime_type,omitempty"`
	CurrentExtensionMIMEType    string     `json:"current_extension_mime_type,omitempty"`
	SuspiciousFileTypeMismatch  *bool      `json:"suspicious_file_type_mismatch,omitempty"`
	PrintJobName                string     `json:"print_job_name,omitempty"`
	PrinterName                 string     `json:"printer_name,omitempty"`
	PrintedFilesBackupPath      string     `json:"printed_files_backup_path,omitempty"`
	RemoteActivity              string     `json:"remote_activity,omitempty"`
	Trusted                     *bool      `json:"trusted,omitempty"`
	LoggedInOperatingSystemUser string     `json:"logged_in_operating_system_user,omitempty"`
}

type ElasticFileEvent struct {
	Event          *Event          `json:"event,omitempty"`
	Timestamp      *time.Time      `json:"@timestamp,omitempty"`
	File           *File           `json:"file,omitempty"`
	User           *User           `json:"user,omitempty"`
	Host           *Host           `json:"host,omitempty"`
	Client         *Client         `json:"client,omitempty"`
	Process        *Process        `json:"process,omitempty"`
	Tab            *Tab            `json:"tab,omitempty"`
	RemovableMedia *RemovableMedia `json:"removable_media,omitempty"`
	EmailDlp       *EmailDlp       `json:"email_dlp,omitempty"`
	Printing       *Printing       `json:"printing,omitempty"`
}

type Event struct {
	Id                 string     `json:"id,omitempty"`
	Type               string     `json:"type,omitempty"`
	Ingested           *time.Time `json:"ingested,omitempty"`
	Created            *time.Time `json:"created,omitempty"`
	Module             string     `json:"module,omitempty"`
	Dataset            []string   `json:"dataset,omitempty"`
	OutsideActiveHours *bool      `json:"outside_active_hours,omitempty"`
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
	Path                        string     `json:"path,omitempty"`
	Name                        string     `json:"name,omitempty"`
	Type                        string     `json:"type,omitempty"`
	Category                    string     `json:"category,omitempty"`
	IdentifiedExtensionCategory string     `json:"identified_extension_category,omitempty"`
	CurrentExtensionCategory    string     `json:"current_extension_category,omitempty"`
	Extension                   []string   `json:"extension,omitempty"` //Array of extensions
	Size                        *int       `json:"size,omitempty"`
	Owner                       []string   `json:"owner,omitempty"` //Array of owners
	Hash                        *Hash      `json:"hash,omitempty"`
	Created                     *time.Time `json:"created,omitempty"`
	Mtime                       *time.Time `json:"mtime,omitempty"`
	Directory                   []string   `json:"directory,omitempty"`
	URL                         *URL       `json:"url,omitempty"`
	Shared                      *bool      `json:"shared,omitempty"`
	SharedWith                  []string   `json:"shared_with,omitempty"`
	SharingTypeAdded            []string   `json:"sharing_type_added,omitempty"`
	CloudDriveId                string     `json:"cloud_drive_id,omitempty"`
	DetectionSourceAlias        string     `json:"detection_source_alias,omitempty"`
	SyncDestination             string     `json:"sync_destination,omitempty"`
	SyncDestinationUser         *User      `json:"sync_destination_user,omitempty"`
	Id                          string     `json:"id,omitempty"`
	IdentifiedExtensionMIMEType string     `json:"identified_extension_mime_type,omitempty"`
	CurrentExtensionMIMEType    string     `json:"current_extension_mime_type,omitempty"`
	SuspiciousFileTypeMismatch  *bool      `json:"suspicious_file_type_mismatch,omitempty"`
	RemoteActivity              string     `json:"remote_activity,omitempty"`
	Trusted                     *bool      `json:"trusted,omitempty"`
}

type User struct {
	Email string `json:"email,omitempty"`
	Id    string `json:"id,omitempty"`
	Actor string `json:"actor,omitempty"`
}

type Host struct {
	Id       string `json:"id,omitempty"`
	Name     string `json:"name,omitempty"`
	Hostname string `json:"hostname,omitempty"`
	User     *User  `json:"user,omitempty"`
}

type Nat struct {
	Ip []string `json:"ip,omitempty"`
}

type Organization struct {
	Name string `json:"name,omitempty"`
}

type AS struct {
	Organization *Organization `json:"organization,omitempty"`
}

type Client struct {
	Ip  string `json:"ip,omitempty"`
	Nat *Nat   `json:"nat,omitempty"`
	Geo *Geo   `json:"geo,omitempty"`
	AS  *AS    `json:"as,omitempty"`
}

type Process struct {
	ProcessOwner string `json:"owner,omitempty"`
	ProcessName  string `json:"name,omitempty"`
}

type Tab struct {
	WindowTitle string `json:"window_title,omitempty"`
	URL         *URL   `json:"url,omitempty"`
}

type RemovableMedia struct {
	Vendor       string `json:"vendor,omitempty"`
	Name         string `json:"name,omitempty"`
	SerialNumber string `json:"serial_number,omitempty"`
	Capacity     *int   `json:"capacity,omitempty"`
	BusType      string `json:"bus_type,omitempty"`
	MediaName    string `json:"media_name,omitempty"`
	VolumeName   string `json:"volume_name,omitempty"`
	PartitionId  string `json:"partition_id,omitempty"`
}

type EmailDlp struct {
	PolicyNames []string `json:"policy_names,omitempty"`
	Subject     string   `json:"subject,omitempty"`
	Sender      string   `json:"sender,omitempty"`
	From        string   `json:"from,omitempty"`
	Recipients  []string `json:"recipients,omitempty"`
}

type Printing struct {
	JobName                string   `json:"job_name,omitempty"`
	Printer                *Printer `json:"printer,omitempty"`
	PrintedFilesBackupPath string   `json:"printed_files_backup_path,omitempty"`
}

type Printer struct {
	Name string `json:"name,omitempty"`
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

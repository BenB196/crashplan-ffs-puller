//Packages provide a module for using the Code42 Crashplan FFS API
package ffs

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

//The main body of a file event record
type FileEvent struct {
	EventId                     string     `json:"eventId"`
	EventType                   string     `json:"eventType"`
	EventTimestamp              *time.Time `json:"eventTimestamp,omitempty"`
	InsertionTimestamp          *time.Time `json:"insertionTimestamp,omitempty"`
	FilePath                    string     `json:"filePath,omitempty"`
	FileName                    string     `json:"fileName"`
	FileType                    string     `json:"fileType,omitempty"`
	FileCategory                string     `json:"fileCategory,omitempty"`
	IdentifiedExtensionCategory string     `json:"identifiedExtensionCategory,omitempty"`
	CurrentExtensionCategory    string     `json:"currentExtensionCategory,omitempty"`
	FileSize                    *int       `json:"fileSize"`
	FileOwner                   []string   `json:"fileOwner,omitempty"` //Array of owners
	Md5Checksum                 string     `json:"md5Checksum,omitempty"`
	Sha256Checksum              string     `json:"sha256Checksum,omitempty"`
	CreatedTimestamp            *time.Time `json:"createdTimestamp,omitempty"`
	ModifyTimestamp             *time.Time `json:"modifyTimestamp,omitempty"`
	DeviceUsername              string     `json:"deviceUsername,omitempty"`
	DeviceUid                   string     `json:"deviceUid,omitempty"`
	UserUid                     string     `json:"userUid,omitempty"`
	OsHostname                  string     `json:"osHostname,omitempty"`
	DomainName                  string     `json:"domainName,omitempty"`
	PublicIpAddress             string     `json:"publicIpAddress,omitempty"`
	PrivateIpAddresses          []string   `json:"privateIpAddresses,omitempty"` //Array of IP address strings
	Actor                       string     `json:"actor,omitempty"`
	DirectoryId                 []string   `json:"directoryId,omitempty"` //An array of something, I am not sure
	Source                      string     `json:"source,omitempty"`
	Url                         string     `json:"url,omitempty"`
	Shared                      string     `json:"shared,omitempty"`
	SharedWith                  []string   `json:"sharedWith,omitempty"` //An array of strings (Mainly Email Addresses)
	SharingTypeAdded            []string   `json:"sharingTypeAdded,omitempty"`
	CloudDriveId                string     `json:"cloudDriveId,omitempty"`
	DetectionSourceAlias        string     `json:"detectionSourceAlias,omitempty"`
	FileId                      string     `json:"fileId,omitempty"`
	Exposure                    []string   `json:"exposure,omitempty"`
	ProcessOwner                string     `json:"processOwner,omitempty"`
	ProcessName                 string     `json:"processName,omitempty"`
	TabWindowTitle              string     `json:"tabWindowTitle,omitempty"`
	TabUrl                      string     `json:"tabUrl,omitempty"`
	RemovableMediaVendor        string     `json:"removableMediaVendor,omitempty"`
	RemovableMediaName          string     `json:"removableMediaName,omitempty"`
	RemovableMediaSerialNumber  string     `json:"removableMediaSerialNumber,omitempty"`
	RemovableMediaCapacity      *int       `json:"removableMediaCapacity,omitempty"`
	RemovableMediaBusType       string     `json:"removableMediaBusType,omitempty"`
	RemovableMediaMediaName     string     `json:"removableMediaMediaName,omitempty"`
	RemovableMediaVolumeName    string     `json:"removableMediaVolumeName,omitempty"`
	RemovableMediaPartitionId   string     `json:"removableMediaPartitionId,omitempty"`
	SyncDestination             string     `json:"syncDestination,omitempty"`
	EmailDLPPolicyNames         []string   `json:"emailDLPPolicyNames,omitempty"`
	EmailDLPSubject             string     `json:"emailDLPSubject,omitempty"`
	EmailDLPSender              string     `json:"emailDLPSender,omitempty"`
	EmailDLPFrom                string     `json:"emailDLPFrom,omitempty"`
	EmailDLPRecipients          []string   `json:"emailDLPRecipients,omitempty"`
	OutsideActiveHours          string     `json:"outsideActiveHours,omitempty"`
	IdentifiedExtensionMIMEType string     `json:"identifiedExtensionMimeType,omitempty"`
	CurrentExtensionMIMEType    string     `json:"currentExtensionMimeType,omitempty"`
	SuspiciousFileTypeMismatch  string     `json:"suspiciousFileTypeMismatch,omitempty"`
}

//Currently recognized csv headers
var csvHeaders = []string{"Event ID", "Event type", "Date Observed (UTC)", "Date Inserted (UTC)", "File path", "Filename", "File type", "File Category", "Identified Extension Category", "Current Extension Category", "File size (bytes)", "File Owner", "MD5 Hash", "SHA-256 Hash", "Create Date", "Modified Date", "Username", "Device ID", "User UID", "Hostname", "Fully Qualified Domain Name", "IP address (public)", "IP address (private)", "Actor", "Directory ID", "Source", "URL", "Shared", "Shared With", "File exposure changed to", "Cloud drive ID", "Detection Source Alias", "File Id", "Exposure Type", "Process Owner", "Process Name", "Tab/Window Title", "Tab URL", "Removable Media Vendor", "Removable Media Name", "Removable Media Serial Number", "Removable Media Capacity", "Removable Media Bus Type", "Removable Media Media Name", "Removable Media Volume Name", "Removable Media Partition Id", "Sync Destination", "Email DLP Policy Names", "Email DLP Subject", "Email DLP Sender", "Email DLP From", "Email DLP Recipients", "Outside Active Hours", "Identified Extension MIME Type", "Current Extension MIME Type", "Suspicious File Type Mismatch"}

//Structs of Crashplan FFS API Authentication Token Return
type AuthData struct {
	Data     AuthToken `json:"data"`
	Error    string    `json:"error,omitempty"`
	Warnings string    `json:"warnings,omitempty"`
}
type AuthToken struct {
	V3UserToken string `json:"v3_user_token"`
}

//Structs for FFS Queries
type Query struct {
	Groups      []Group `json:"groups"`
	GroupClause string  `json:"groupClause,omitempty"`
	PgNum       int     `json:"pgNum,omitempty"`
	PgSize      int     `json:"pgSize,omitempty"`
	SrtDir      string  `json:"srtDir,omitempty"`
	SrtKey      string  `json:"srtKey,omitempty"`
}

type Group struct {
	Filters      []Filter `json:"filters"`
	FilterClause string   `json:"filterClause,omitempty"`
}

type Filter struct {
	Operator string `json:"operator"`
	Term     string `json:"term"`
	Value    string `json:"value"`
}

/*
GetAuthData - Function to get the Authentication data (mainly the authentication token) which will be needed for the rest of the API calls
The authentication token is good for up to 1 hour before it expires
*/
func GetAuthData(uri string, username string, password string) (AuthData, error) {
	//Build HTTP GET request
	req, err := http.NewRequest("GET", uri, nil)

	//Return nil and err if Building of HTTP GET request fails
	if err != nil {
		return AuthData{}, err
	}

	//Set Basic Auth Header
	req.SetBasicAuth(username, password)
	//Set Accept Header
	req.Header.Set("Accept", "application/json")

	//Make the HTTP Call
	resp, err := http.DefaultClient.Do(req)

	//Return nil and err if Building of HTTP GET request fails
	if err != nil {
		return AuthData{}, err
	}

	defer resp.Body.Close()

	//Return err if status code != 200
	if resp.StatusCode != http.StatusOK {
		return AuthData{}, errors.New("Error with Authentication Token GET: " + resp.Status)
	}

	//Create AuthData variable
	var authData AuthData

	respData := resp.Body

	responseBytes, _ := ioutil.ReadAll(respData)

	if strings.Contains(string(responseBytes), "Service Under Maintenance") {
		return AuthData{}, errors.New("error: auth api service is under maintenance")
	}

	//Decode the resp.Body into authData variable
	err = json.Unmarshal(responseBytes, &authData)

	//Return nil and err if decoding of resp.Body fails
	if err != nil {
		return AuthData{}, err
	}

	//Return AuthData
	return authData, nil
}

//TODO create Global Function for calling getFileEvents with CSV url formatting (Priority, as will likely continue to be supported by Code42)
/*
csvLineToFileEvent - Converts a CSV Line into a File Event Struct
[]string - csv line. DO NOT PASS Line 0 (Headers) if they exist
This function contains panics in order to prevent messed up CSV parsing
*/
func csvLineToFileEvent(csvLine []string) FileEvent {
	//Convert []string to designated variables
	eventId := csvLine[0]
	eventType := csvLine[1]
	eventTimestampString := csvLine[2]     //Converted to time below
	insertionTimestampString := csvLine[3] //Converted to time below
	filePath := csvLine[4]
	fileName := csvLine[5]
	fileType := csvLine[6]
	fileCategory := csvLine[7]
	identifiedExtensionCategory := csvLine[8]
	currentExtensionCategory := csvLine[9]
	fileSizeString := csvLine[10]  //Converted to int below
	fileOwnerString := csvLine[11] //Converted to slice below
	md5Checksum := csvLine[12]
	sha256Checksum := csvLine[13]
	createdTimestampString := csvLine[14] //Converted to time below
	modifyTimestampString := csvLine[15]  //Converted to time below
	deviceUserName := csvLine[16]
	deviceUid := csvLine[17]
	userUid := csvLine[18]
	osHostName := csvLine[19]
	domainName := csvLine[20]
	publicIpAddress := csvLine[21]
	privateIpAddressesString := csvLine[22] //Converted to slice below
	actor := csvLine[23]
	directoryIdString := csvLine[24] //Converted to slice below
	source := csvLine[25]
	url := csvLine[26]
	shared := csvLine[27]
	sharedWithString := csvLine[28]       //Converted to slice below
	sharingTypeAddedString := csvLine[29] //Converted to slice below
	cloudDriveId := csvLine[30]
	detectionSourceAlias := csvLine[31]
	fileId := csvLine[32]
	exposureString := csvLine[33] //Convert to slice below
	processOwner := csvLine[34]
	processName := csvLine[35]
	tabWindowTitle := csvLine[36]
	tabUrl := csvLine[37]
	removableMediaVendor := csvLine[38]
	removableMediaName := csvLine[39]
	removableMediaSerialNumber := csvLine[40]
	removableMediaCapacityString := csvLine[41] //Converted to int below
	removableMediaBusType := csvLine[42]
	removableMediaMediaName := csvLine[43]
	removableMediaVolumeName := csvLine[44]
	removableMediaPartitionId := csvLine[45]
	syncDestination := csvLine[46]
	emailDLPPolicyNamesString := csvLine[47] //Convert to slice below
	emailDLPSubject := csvLine[48]
	emailDLPSender := csvLine[49]
	emailDLPFrom := csvLine[50]
	emailDLPRecipientsString := csvLine[51] //Convert to slice below
	outsideActiveHours := csvLine[52]
	identifiedExtensionMimeType := csvLine[53]
	currentExtensionMimeType := csvLine[54]
	suspiciousFileTypeMismatch := csvLine[55]

	//Set err
	var err error

	//Convert eventTimeStamp to time
	var eventTimeStamp time.Time
	if eventTimestampString != "" {
		eventTimeStamp, err = time.Parse(time.RFC3339Nano, eventTimestampString)

		//Panic if this fails, that means something is wrong with CSV handling
		if err != nil {
			log.Println("Error parsing eventTimeStampString, something must be wrong with CSV parsing.")
			log.Println(csvLine)
			panic(err)
		}
	}

	//Convert insertionTimestamp to time
	var insertionTimestamp time.Time
	if insertionTimestampString != "" {
		insertionTimestamp, err = time.Parse(time.RFC3339Nano, insertionTimestampString)

		//Panic if this fails, that means something is wrong with CSV handling
		if err != nil {
			log.Println("Error parsing insertionTimestampString, something must be wrong with CSV parsing.")
			log.Println(csvLine)
			panic(err)
		}
	}

	//Convert fileSizeString to int
	var fileSize int
	if fileSizeString != "" {
		fileSize, err = strconv.Atoi(fileSizeString)

		//Panic if this fails, that means something is wrong with CSV handling
		if err != nil {
			log.Println("Error parsing fileSizeString, something must be wrong with CSV parsing.")
			log.Println(csvLine)
			panic(err)
		}
	}

	//Convert fileOwnerString to string slice
	var fileOwner []string
	if fileOwnerString != "" {
		fileOwner = strings.Split(fileOwnerString, ",")
	}

	//Convert createdTimestamp to time
	var createdTimestamp time.Time
	if createdTimestampString != "" {
		createdTimestamp, err = time.Parse("2006-01-02 15:04:05", createdTimestampString)

		//Panic if this fails, that means something is wrong with CSV handling
		if err != nil {
			log.Println("Error parsing createdTimestampString, something must be wrong with CSV parsing.")
			log.Println(csvLine)
			panic(err)
		}
	}

	//Convert modifyTimestamp to time
	var modifyTimestamp time.Time
	if modifyTimestampString != "" {
		modifyTimestamp, err = time.Parse("2006-01-02 15:04:05", modifyTimestampString)

		//Panic if this fails, that means something is wrong with CSV handling
		if err != nil {
			log.Println("Error parsing modifyTimestampString, something must be wrong with CSV parsing.")
			log.Println(csvLine)
			panic(err)
		}
	}

	//Convert privateIpAddresses to string slice
	var privateIpAddresses []string
	if privateIpAddressesString != "" {
		privateIpAddressesString := strings.Replace(privateIpAddressesString, "\n", "", -1)
		privateIpAddresses = strings.Split(privateIpAddressesString, ",")
	}

	//Convert directoryId to string slice
	var directoryId []string
	if directoryIdString != "" {
		directoryIdString := strings.Replace(directoryIdString, "\n", "", -1)
		directoryId = strings.Split(directoryIdString, ",")
	}

	//Convert sharedWith to string slice
	var sharedWith []string
	if sharedWithString != "" {
		sharedWithString := strings.Replace(sharedWithString, "\n", "", -1)
		sharedWith = strings.Split(sharedWithString, ",")
	}

	//Convert sharingTypeAdded to string slice
	var sharingTypeAdded []string
	if sharingTypeAddedString != "" {
		sharingTypeAddedString := strings.Replace(sharingTypeAddedString, "\n", "", -1)
		sharingTypeAdded = strings.Split(sharingTypeAddedString, ",")
	}

	//Convert exposure to string slice
	var exposure []string
	if exposureString != "" {
		exposureString := strings.Replace(exposureString, "\n", "", -1)
		exposure = strings.Split(exposureString, ",")
	}

	//Convert emailDLPRecipients to string slice
	var emailDLPRecipients []string
	if emailDLPRecipientsString != "" {
		emailDLPRecipientsString := strings.Replace(emailDLPRecipientsString, "\n", "", -1)
		emailDLPRecipients = strings.Split(emailDLPRecipientsString, ",")
	}

	//Convert emailDLPPolicyNames to string slice
	var emailDLPPolicyNames []string
	if emailDLPPolicyNamesString != "" {
		emailDLPPolicyNamesString := strings.Replace(emailDLPPolicyNamesString, "\n", "", -1)
		emailDLPPolicyNames = strings.Split(emailDLPPolicyNamesString, ",")
	}

	//Convert removableMediaCapacity to int
	var removableMediaCapacity int
	if removableMediaCapacityString != "" {
		removableMediaCapacity, err = strconv.Atoi(removableMediaCapacityString)

		//Panic if this fails, that means something is wrong with CSV handling
		if err != nil {
			log.Println("Error parsing removableMediaCapacityString, something must be wrong with CSV parsing.")
			log.Println(csvLine)
			panic(err)
		}
	}

	var fileEvent FileEvent

	//Build FileEvent struct
	fileEvent = FileEvent{
		EventId:                     eventId,
		EventType:                   eventType,
		EventTimestamp:              &eventTimeStamp,
		InsertionTimestamp:          &insertionTimestamp,
		FilePath:                    filePath,
		FileName:                    fileName,
		FileType:                    fileType,
		FileCategory:                fileCategory,
		IdentifiedExtensionCategory: identifiedExtensionCategory,
		CurrentExtensionCategory:    currentExtensionCategory,
		FileSize:                    &fileSize,
		FileOwner:                   fileOwner,
		Md5Checksum:                 md5Checksum,
		Sha256Checksum:              sha256Checksum,
		CreatedTimestamp:            &createdTimestamp,
		ModifyTimestamp:             &modifyTimestamp,
		DeviceUsername:              deviceUserName,
		DeviceUid:                   deviceUid,
		UserUid:                     userUid,
		OsHostname:                  osHostName,
		DomainName:                  domainName,
		PublicIpAddress:             publicIpAddress,
		PrivateIpAddresses:          privateIpAddresses,
		Actor:                       actor,
		DirectoryId:                 directoryId,
		Source:                      source,
		Url:                         url,
		Shared:                      shared,
		SharedWith:                  sharedWith,
		SharingTypeAdded:            sharingTypeAdded,
		CloudDriveId:                cloudDriveId,
		DetectionSourceAlias:        detectionSourceAlias,
		FileId:                      fileId,
		Exposure:                    exposure,
		ProcessOwner:                processOwner,
		ProcessName:                 processName,
		TabWindowTitle:              tabWindowTitle,
		TabUrl:                      tabUrl,
		RemovableMediaVendor:        removableMediaVendor,
		RemovableMediaName:          removableMediaName,
		RemovableMediaSerialNumber:  removableMediaSerialNumber,
		RemovableMediaCapacity:      &removableMediaCapacity,
		RemovableMediaBusType:       removableMediaBusType,
		RemovableMediaMediaName:     removableMediaMediaName,
		RemovableMediaVolumeName:    removableMediaVolumeName,
		RemovableMediaPartitionId:   removableMediaPartitionId,
		SyncDestination:             syncDestination,
		EmailDLPPolicyNames:         emailDLPPolicyNames,
		EmailDLPSubject:             emailDLPSubject,
		EmailDLPSender:              emailDLPSender,
		EmailDLPFrom:                emailDLPFrom,
		EmailDLPRecipients:          emailDLPRecipients,
		OutsideActiveHours:          outsideActiveHours,
		IdentifiedExtensionMIMEType: identifiedExtensionMimeType,
		CurrentExtensionMIMEType:    currentExtensionMimeType,
		SuspiciousFileTypeMismatch:  suspiciousFileTypeMismatch,
	}

	//set eventTimestamp to nil if empty string
	if eventTimestampString == "" {
		fileEvent.EventTimestamp = nil
	}

	//set insertionTimestamp to nil if empty
	if insertionTimestampString == "" {
		fileEvent.InsertionTimestamp = nil
	}

	//set createdTimestamp to nil if empty
	if createdTimestampString == "" {
		fileEvent.CreatedTimestamp = nil
	}

	//set modifyTimestamp to nil if empty
	if modifyTimestampString == "" {
		fileEvent.ModifyTimestamp = nil
	}

	//set fileSize to nil if empty
	if fileSizeString == "" {
		fileEvent.FileSize = nil
	}

	//set removableMediaCapacity to nil if empty
	if removableMediaCapacityString == "" {
		fileEvent.RemovableMediaCapacity = nil
	}

	return fileEvent
}

//TODO create Global Function for calling getFileEvents with JSON url formatting (this may be not be needed, Code42 seems to frown upon using this for pulling large amounts of events.)

/*
getFileEvents - Function to get the actual event records from FFS
authData - authData struct which contains the authentication API token
ffsURI - the URI for where to pull the FFS events
query - query struct which contains the actual FFS query and a golang valid form
This function contains a panic if the csv columns do not match the currently specified list.
This is to prevent data from being messed up during parsing.
*/
func GetFileEvents(authData AuthData, ffsURI string, query Query) ([]FileEvent, error) {

	//Validate jsonQuery is valid JSON
	ffsQuery, err := json.Marshal(query)
	if err != nil {
		return nil, errors.New("jsonQuery is not in a valid json format")
	}

	//Make sure authData token is not ""
	if authData.Data.V3UserToken == "" {
		return nil, errors.New("authData cannot be nil")
	}

	//Query ffsURI with authData API token and jsonQuery body
	req, err := http.NewRequest("POST", ffsURI, bytes.NewReader(ffsQuery))

	//Handle request errors
	if err != nil {
		return nil, err
	}

	//Set request headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "v3_user_token "+authData.Data.V3UserToken)

	//Get Response
	resp, err := http.DefaultClient.Do(req)

	//Handle response errors
	if err != nil {
		return nil, err
	}

	//defer body close
	defer resp.Body.Close()

	//Make sure http status code is 200
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("Error with gathering file events POST: " + resp.Status)
	}

	//Read Response Body as CSV
	reader := csv.NewReader(resp.Body)
	reader.Comma = ','

	//Read body into variable
	data, err := reader.ReadAll()

	//Handle reader errors
	if err != nil {
		return nil, err
	}

	var fileEvents []FileEvent

	//Loop through CSV lines
	var wg sync.WaitGroup
	wg.Add(len(data))
	go func() {
		for lineNumber, lineContent := range data {
			if lineNumber != 0 {
				//Convert CSV line to file events and add to slice
				fileEvents = append(fileEvents, csvLineToFileEvent(lineContent))
			} else {
				//Validate that the columns have not changed
				differences := difference(lineContent, csvHeaders)

				if len(differences) > 0 {
					panic(errors.New("number of columns in CSV file does not match expected number, API changed, panicking to keep data integrity. columns that changed: " + strings.Join(differences, ",")))
				}
			}
			wg.Done()
		}
	}()

	wg.Wait()

	return fileEvents, nil
}

/*
Calculate the difference between two different slices
Used in this case to tell if the csv columns have changed
*/
func difference(slice1 []string, slice2 []string) []string {
	var diff []string

	// Loop two times, first to find slice1 strings not in slice2,
	// second loop to find slice2 strings not in slice1
	for i := 0; i < 2; i++ {
		for _, s1 := range slice1 {
			found := false
			for _, s2 := range slice2 {
				if s1 == s2 {
					found = true
					break
				}
			}
			// String not found. We add it to return slice
			if !found {
				diff = append(diff, s1)
			}
		}
		// Swap the slices, only if it was the first loop
		if i == 0 {
			slice1, slice2 = slice2, slice1
		}
	}
	return diff
}

package eventOutput

import (
	"bufio"
	"crashplan-ffs-puller/config"
	"crashplan-ffs-puller/ffsEvent"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

func WriteEvents (ffsEvents []ffsEvent.FFSEvent, query config.FFSQuery) error {
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
	var ffsEventsString string
	ffsEventsStringPointer := &ffsEventsString

	log.Println("Marshaling events")
	var wg sync.WaitGroup
	wg.Add(len(ffsEvents))
	go func() {
		for _, event := range ffsEvents {
			ffsEventBytes, err := json.Marshal(event)

			if err != nil {
				panic(errors.New("error: marshaling ffs event: " + err.Error()))
			}

			*ffsEventsStringPointer = *ffsEventsStringPointer + string(ffsEventBytes) + "\n"
			wg.Done()
		}
	}()

	wg.Wait()

	//Write events to file
	if ffsEventsString != "" {
		log.Println("Writing file")
		_, err := w.WriteString(ffsEventsString)

		if err != nil {
			return errors.New("error: writing events to file: " + fileName + " " + err.Error())
		}
	}

	log.Println("Syncing file")

	err = w.Flush()

	if err != nil {
		return errors.New("error: flushing file: " + fileName + " " + err.Error())
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
				onOrAfter, err := time.Parse(time.RFC3339Nano,filters.Value)
				if err != nil {
					return "", errors.New("error getting on or after value for file name: " + err.Error())
				}
				fileName = fileName + "A" + onOrAfter.Format("2006.01.02.15.04.05.000")
			} else if filters.Operator == "ON_OR_BEFORE" {
				//Get value in golang time
				onOrBefore, err := time.Parse(time.RFC3339Nano,filters.Value)
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
	OnOrAfter	time.Time
	OnOrBefore	time.Time
}

//In progress query struct using strings
type InProgressQueryString struct {
	OnOrAfter	string
	OnOrBefore	string
}

func WriteInProgressQueries(query config.FFSQuery, inProgressQueries *[]InProgressQuery) error {
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
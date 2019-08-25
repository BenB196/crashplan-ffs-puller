package eventOutput

import (
	"bufio"
	ffs "crashplan-ffs-go-pkg"
	"crashplan-ffs-puller/ffsEvent"
	"encoding/json"
	"errors"
	"os"
	"time"
)

func WriteEvents (ffsEvents []ffsEvent.FFSEvent, outputLocation string, query ffs.Query) error {
	//Error if ffsEvents is nil, this should not be called if there are no ffsEvents
	if ffsEvents == nil {
		return errors.New("error: ffsEvents is nil")
	}

	//Check if outputLocation is provided
	if outputLocation == "" {
		return errors.New("error: no output location provided")
	}

	//Generate filename
	fileName, err := generateFileName(query)

	//handle errs
	if err != nil {
		return err
	}

	//Create output file
	file, err := os.Create(outputLocation + fileName)

	defer func() {
		if err := file.Close(); err != nil {
			 panic(errors.New("error: closing file: " + outputLocation + fileName + " " + err.Error()))
		}
	}()

	if err != nil {
		return errors.New("error: creating file: " + outputLocation + fileName + " " + err.Error())
	}

	//Create buffer writer
	w := bufio.NewWriter(file)

	//Build ffsEvents string
	var ffsEventsString string
	for _, event := range ffsEvents {
		ffsEventBytes, err := json.Marshal(event)

		if err != nil {
			return errors.New("error: marshaling ffs event: " + err.Error())
		}

		ffsEventsString = ffsEventsString + string(ffsEventBytes) + "\n"
	}

	//Write events to file
	if ffsEventsString != "" {
		_, err := w.WriteString(ffsEventsString)

		if err != nil {
			return errors.New("error: writing events to file: " + outputLocation + fileName + " " + err.Error())
		}
	}

	err = w.Flush()

	if err != nil {
		return errors.New("error: flushing file: " + outputLocation + fileName + " " + err.Error())
	}

	return nil
}

func generateFileName(query ffs.Query) (string, error) {
	//Check if query.Groups is not 0, will need to generate filename
	if len(query.Groups) == 0 {
		return "", errors.New("error: no groups provided")
	}

	//Create filename var
	var fileName string

	for _, groups := range query.Groups {
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

	//Validate that a filename was generated
	if fileName == "" {
		return "", errors.New("failed to generate file name properly")
	} else if len(fileName) > 248 {
		//truncate filename if longer than 248 characters
		fileName = fileName[0:248]
	}

	//Add file extension
	//TODO add support for other outputs?
	fileName = fileName + ".json"

	return fileName, nil
}
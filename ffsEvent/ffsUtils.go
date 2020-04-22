package ffsEvent

import (
	"errors"
	"github.com/BenB196/crashplan-ffs-go-pkg"
	"github.com/BenB196/crashplan-ffs-puller/config"
	"github.com/BenB196/crashplan-ffs-puller/eventOutput"
	"golang.org/x/net/publicsuffix"
	"log"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func getOnOrTime(beforeAfter string, query ffs.Query) (time.Time, error) {
	for _, group := range query.Groups {
		for _, filter := range group.Filters {
			if beforeAfter == "before" && filter.Operator == "ON_OR_BEFORE" {
				if filter.Value == "" || filter.Value == (time.Time{}.String()) {
					return time.Time{}, nil
				} else {
					return time.Parse(time.RFC3339Nano, filter.Value)
				}
			} else if beforeAfter == "after" && filter.Operator == "ON_OR_AFTER" {
				if filter.Value == "" || filter.Value == (time.Time{}.String()) {
					return time.Time{}, nil
				} else {
					return time.Parse(time.RFC3339Nano, filter.Value)
				}
			}
		}
	}

	return time.Time{}, nil
}

func getOnOrBeforeAndAfter(query config.FFSQuery) (*eventOutput.InProgressQuery, error) {
	onOrAfter, err := getOnOrTime("after", query.Query)

	if err != nil {
		return nil, errors.New("error parsing onOrAfter time for ffs query: " + query.Name + " " + err.Error())
	}

	onOrBefore, err := getOnOrTime("before", query.Query)

	if err != nil {
		return nil, errors.New("error parsing onOrBefore time for ffs query: " + query.Name + " " + err.Error())
	}

	return &eventOutput.InProgressQuery{
		OnOrAfter:  onOrAfter,
		OnOrBefore: onOrBefore,
	}, nil
}

func setOnOrTime(beforeAfter string, query ffs.Query, timeStamp time.Time) ffs.Query {
	for k, group := range query.Groups {
		for i, filter := range group.Filters {
			if beforeAfter == "before" && filter.Operator == "ON_OR_BEFORE" {
				query.Groups[k].Filters[i].Value = timeStamp.Format("2006-01-02T15:04:05.000Z")
			} else if beforeAfter == "after" && filter.Operator == "ON_OR_AFTER" {
				query.Groups[k].Filters[i].Value = timeStamp.Format("2006-01-02T15:04:05.000Z")
			}
		}
	}

	return query
}

func setOnOrBeforeAndAfter(query config.FFSQuery, beforeTime time.Time, afterTime time.Time) config.FFSQuery {
	query.Query = setOnOrTime("before", query.Query, beforeTime)
	query.Query = setOnOrTime("after", query.Query, afterTime)

	return query
}

//Logic for setting the correct times
//TODO make sure on or before never exceeds time.Now -15 minutes. This is what Code42 sets as expected time for logs to be ready for pulling
//If len(inProgressQueries) == 0
//then check last completed query
//If last completed query is "empty"
//then get ffs query times
//If ffs query on or after is empty
//then set to time.now
//else do nothing
//If ffs query on or before is empty
//then set to on or after + query time Interval
//else this is max time and should not be exceeded TODO save this to a "max time variable" that is checked only on program startup
//else set time based off of last completed query + time gap
//else get last inProgressQuery
//then check if last completed query is set
//if last completed query is set
//then compare last in progress query to last completed query and see which is newer
//else set time based off of last in progress query + time gap

func calculateTimeStamps(inProgressQueries []eventOutput.InProgressQuery, lastCompletedQuery eventOutput.InProgressQuery, query config.FFSQuery, maxTime time.Time) (config.FFSQuery, bool, error) {
	//Create variable which will be used to store the latest query to have run
	var lastQueryInterval eventOutput.InProgressQuery

	//Set timezone
	loc, _ := time.LoadLocation("UTC")
	timeNow := time.Now().Add(-15 * time.Minute).In(loc)

	//Get time gap as a duration
	timeGap, err := time.ParseDuration(query.TimeGap)
	if err != nil {
		return query, false, err
	}

	if len(inProgressQueries) == 0 {
		if lastCompletedQuery != (eventOutput.InProgressQuery{}) {
			lastQueryInterval = lastCompletedQuery
		} else {
			currentQuery, err := getOnOrBeforeAndAfter(query)
			if err != nil {
				return query, false, err
			}
			if currentQuery.OnOrAfter == (time.Time{}) {
				lastQueryInterval = eventOutput.InProgressQuery{
					OnOrAfter:  timeNow.Add(-timeGap),
					OnOrBefore: timeNow.Add(timeGap),
				}
			} else {
				lastQueryInterval = eventOutput.InProgressQuery{
					OnOrAfter:  currentQuery.OnOrAfter.Add(1 * time.Millisecond),
					OnOrBefore: currentQuery.OnOrAfter.Add(1 * time.Millisecond).Add(timeGap),
				}
			}
		}
	} else {
		lastInProgressQuery := inProgressQueries[len(inProgressQueries)-1]
		if lastCompletedQuery != (eventOutput.InProgressQuery{}) {
			lastQueryInterval = getNewerTimeQuery(lastInProgressQuery, lastCompletedQuery)
		} else {
			lastQueryInterval = lastInProgressQuery
		}
	}

	//set time variables
	newOnOrAfter := lastQueryInterval.OnOrBefore.Add(1 * time.Millisecond)
	newOnOrBefore := lastQueryInterval.OnOrBefore.Add(1 * time.Millisecond).Add(timeGap)

	//TODO implement a check for "max time"
	var done bool
	if maxTime != (time.Time{}) {
		if maxTime.Sub(newOnOrAfter) <= 0 {
			done = true
		} else if maxTime.Sub(newOnOrBefore) <= 0 {
			newOnOrBefore = maxTime
		}
	}

	//Truncate time if within the 15 minute no go window
	if timeNow.Sub(newOnOrBefore) <= 0 {
		newOnOrBefore = timeNow
	}

	//Increment time
	return setOnOrBeforeAndAfter(query, newOnOrBefore, newOnOrAfter), done, nil
}

func getNewerTimeQuery(lastInProgressQuery eventOutput.InProgressQuery, lastCompletedQuery eventOutput.InProgressQuery) eventOutput.InProgressQuery {
	if lastCompletedQuery.OnOrBefore.Sub(lastInProgressQuery.OnOrAfter) <= 0 {
		return lastInProgressQuery
	} else {
		return lastCompletedQuery
	}
}

func getUrlInfo(urlFull string) *eventOutput.URL {
	if urlFull == "" {
		return nil
	}

	var eventUrl eventOutput.URL

	eventUrl.Full = urlFull

	u, err := url.Parse(urlFull)

	if err != nil {
		log.Println("Error processing Event URL; error: " + err.Error() + ", URL: " + urlFull)
	} else {
		//set scheme
		eventUrl.Scheme = u.Scheme
		//set username
		eventUrl.Username = u.User.Username()
		//set password
		eventUrl.Password, _ = u.User.Password()
		//get host/port
		host, port, err := net.SplitHostPort(u.Host)
		if err != nil {
			log.Println("Error processing host from Event Url; error: " + err.Error() + ", Host: " + u.Host)
		}
		//set domain
		eventUrl.Domain = host
		//set port
		if port == "" {
			eventUrl.Port = nil
		} else {
			portNum, err := strconv.Atoi(port)
			if err != nil {
				log.Println("Error processing port from Event Url; error: " + err.Error() + ", Port: " + port)
			}
			eventUrl.Port = &portNum
		}
		//set path
		eventUrl.Path = u.RawPath
		//set extension
		if strings.Contains(eventUrl.Path, ".") {
			pathParts := strings.Split(eventUrl.Path,".")
			eventUrl.Extension = pathParts[len(pathParts)-1]
		}
		//set fragment
		eventUrl.Fragment = u.Fragment
		//set query
		eventUrl.Query = u.RawQuery
		//set registered domain
		eventUrl.RegisteredDomain, err = publicsuffix.EffectiveTLDPlusOne(eventUrl.Domain)
		if err != nil {
			log.Println("Error getting Registered Domain; error: " + err.Error() + ", Domain: " + eventUrl.Domain)
		}
		//set top level domain
		eventUrl.TopLevelDomain, _ = publicsuffix.PublicSuffix(eventUrl.Domain)
	}
	return &eventUrl
}
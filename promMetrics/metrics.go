package promMetrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	eventsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "crashplan_ffs_puller_events_total",
		Help: "The total number of processed FFS events",
	})
	inProgressQueries = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "crashplan_ffs_puller_in_progress_queries",
		Help: "The current number of in progress queries",
	})
)

func IncrementEventsProcessed(numberOfEvents int)  {
	eventsProcessed.Add(float64(numberOfEvents))
}

func AdjustInProgressQueries(numberOfInProgressQueries int) {
	inProgressQueries.Add(float64(numberOfInProgressQueries))
}
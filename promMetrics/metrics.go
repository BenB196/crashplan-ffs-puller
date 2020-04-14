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
	requestsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ip_api_proxy_requests_total",
		Help: "The total number of requests processed",
	})
	batchRequestsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ip_api_proxy_batch_requests_processed_total",
		Help: "The total number of batch requests processed",
	})
	singleRequestsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ip_api_proxy_single_requests_processed_total",
		Help: "The total number of single requests processed",
	})
	queriesProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ip_api_proxy_queries_total",
		Help: "The total number of queries processed",
	})
	batchQueriesProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ip_api_proxy_batch_queries_processed_total",
		Help: "The total number of batch queries processed",
	})
	singleQueriesProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ip_api_proxy_single_queries_processed_total",
		Help: "The total number of single queries processed",
	})
	requestsForwarded = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ip_api_proxy_requests_forwarded_total",
		Help: "The total number of requests forwarded to IP-API",
	})
	queriesForwarded = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ip_api_proxy_queries_forwarded_total",
		Help: "The total number of queries forwarded to IP-API",
	})
	queriesCachedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ip_api_proxy_queries_cached_total",
		Help: "The total number of queries that have been cached locally",
	})
	queriesCachedCurrent = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ip_api_proxy_queries_in_cache",
		Help: "The current number of unique queries in the cache currently",
	})
	cacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ip_api_proxy_cache_hits_total",
		Help: "The total number of times that cache has served up a request",
	})
	successfulQueries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ip_api_proxy_successful_queries_total",
		Help: "The total number of successfully fulfilled queries",
	})
	successfulBatchQueries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ip_api_proxy_successful_batch_queries_total",
		Help: "The total number of successfully fulfilled batch queries",
	})
	successfulSingleQueries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ip_api_proxy_successful_single_queries_total",
		Help: "The total number of successfully fulfilled single queries",
	})
	failedRequests = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ip_api_proxy_failed_requests_total",
		Help: "The total number of failed requests",
	})
	failedBatchRequests = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ip_api_proxy_failed_batch_requests_total",
		Help: "The total number of failed batch requests",
	})
	failedSingleRequests = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ip_api_proxy_failed_single_requests_total",
		Help: "The total number of failed single requests",
	})
	failedQueries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ip_api_proxy_failed_queries_total",
		Help: "The total number of failed queries",
	})
	failedBatchQueries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ip_api_proxy_failed_batch_queries_total",
		Help: "The total number of failed batch queries",
	})
	failedSingleQueries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ip_api_proxy_failed_single_queries_total",
		Help: "The total number of failed single queries",
	})
	handlerRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ip_api_proxy_handler_requests_total",
		Help: "Total number of requests by HTTP status code",
	},
		[]string{"code"},
	)
)

func IncrementEventsProcessed(numberOfEvents int)  {
	eventsProcessed.Add(float64(numberOfEvents))
}

func IncreaseInProgressQueries() {
	inProgressQueries.Inc()
}

func DecreaseInProgressQueries() {
	inProgressQueries.Dec()
}

func IncrementRequestsProcessed() {
	requestsProcessed.Inc()
}

func IncrementBatchRequestsProcessed() {
	batchRequestsProcessed.Inc()
}

func IncrementSingleRequestsProcessed() {
	singleRequestsProcessed.Inc()
}

func IncrementQueriesProcessed() {
	queriesProcessed.Inc()
}

func IncrementBatchQueriesProcessed() {
	batchQueriesProcessed.Inc()
}

func IncrementSingleQueriesProcessed() {
	singleQueriesProcessed.Inc()
}

func IncrementRequestsForwarded() {
	requestsForwarded.Inc()
}

func IncrementQueriesForwarded()  {
	queriesForwarded.Inc()
}

func IncrementQueriesCachedTotal()  {
	queriesCachedTotal.Inc()
}

func IncrementQueriesCachedCurrent() {
	queriesCachedCurrent.Inc()
}

func DecreaseQueriesCachedCurrent()  {
	queriesCachedCurrent.Dec()
}

func IncrementCacheHits() {
	cacheHits.Inc()
}

func IncrementSuccessfulQueries()  {
	successfulQueries.Inc()
}

func IncrementSuccessfulBatchQueries() {
	successfulBatchQueries.Inc()
}

func IncrementSuccessfulSingeQueries() {
	successfulSingleQueries.Inc()
}

func IncrementFailedRequests() {
	failedRequests.Inc()
}

func IncrementFailedBatchRequests()  {
	failedBatchRequests.Inc()
}

func IncrementFailedSingleRequests()  {
	failedSingleRequests.Inc()
}

func IncrementFailedQueries()  {
	failedQueries.Inc()
}

func IncrementFailedBatchQueries() {
	failedBatchQueries.Inc()
}

func IncrementFailedSingleQueries() {
	failedSingleQueries.Inc()
}

func IncrementHandlerRequests(code string)  {
	handlerRequests.With(prometheus.Labels{"code":code}).Inc()
}
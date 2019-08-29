#crashplan-ffs-puller

A Golang application used for pulling [Code42's](https://www.code42.com/) [Crashplan](https://www.crashplan.com/en-us/) [File Forensic Search (FFS)](https://support.code42.com/Administrator/Cloud/Administration_console_reference/Forensic_File_Search_reference_guide) logs from their API.

#TODOs

0. Finish writing this README
1. Add support for direct Elasticsearch output type
2. Add integration with [IP-API](http://ip-api.com/) to lookup public IP addresses
3. Add tests for functions (many of these are weird and idk if they work in all situations)

##TODOs (Maybe)

1. Add ability to use yaml/yml configuration files
2. Add the ability to use the regular json FFS API endpoint
3. Add some sort of file hash lookup that could provide threat intelligence (tried this with [OTX](https://www.alienvault.com/open-threat-exchange) before and failed pretty bad).
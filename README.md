# file-system-time-series-database
This module allows using a file system as a time series database.  It is efficient
for these queries:

* Get a range of data for an identifier (such as a user id)
* Get the most recent time for an identifer (such as a user id)
* Get the earliest time for an identifer (such as a user id)

The number of files in a directory is attempting to be limited to 768,
though this may be exceeded occasionally.  NFS allows 65535.

## File System Format
The data is laid out in the following format, which shards the data
by month:

    /year/group1/group2/8ofid/8ofid/8ofid/id/year-month-day.extension

For example, for user `deadbeef` with a JSON file, group1 `garmin`,
group2 `sleeps`:

    /2020/garmin/sleeps/de/ad/be/deadbeef/2020-08-09.json

this will support billions of users with ease, provided the user ID
has sufficient randomness.

## Performance
### Performance Assumptions.
The file system is assumed to have about 10mS of latency or less.

### Parallelism
Operations are meant to be parallized, so for example if you are looking
for the latest data the year direcotry would be queried for existence
in parallel from the start epoch (2020) to today's current date, using
`Promise.map()` to parallalize.


# Usage
This package was developed to work with AWS's EFS on Lambda, though the file system 
handle is passed into this library so it doesn't really care.

## Module Usage

1. Add Module:

        npm install --save filesystem-timeseries-db

1. Require Statement:

        const FsTimeSeriesDB = require('filesystem-timeseries-db');

1. General usage:

        const fileSystem = fileHandle;
        const dbInstance = Object.create(FsTimeSeriesDB).setOptions({rootPath: rootPath});


# Testing
## Interactive Testing
1. Run Docker Compose:

        docker-compose run app  /bin/bash

1. Navigate to the directory where the source code is located:

        cd /home/code       

1. Run NPM install

        npm install
        
1. Run the unit tests

        npm test test/*.js



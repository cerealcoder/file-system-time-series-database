'use strict';

const assert = require('assert');
const _ = require('underscore');
const {gzip, ungzip} = require('node-gzip');
const Promise = require('bluebird');
const fs = require('fs').promises;
const mkdirp = require('mkdirp');
const bs = require('binary-search');
const util = require('util');

const FsTimeSeriesDB = Object.create({});
exports = module.exports = FsTimeSeriesDB;


// 
// set options
//
FsTimeSeriesDB.options = {
  rootPath: null,
};

FsTimeSeriesDB.setOptions = function(options) {
  this.options = this.verifyOptions(options);
  return this;
};

FsTimeSeriesDB.verifyOptions = function(options) {
  // mandatory options
  assert(options, 'there must be options set');
  assert(options.rootPath, 'root path must be defined');

  return options;
};

/**
 * Note:  Events are stored as a file that has epochTimeMilliSec as the name of the file.
 * This means you can only *put* about 65000 discrete events per year.
 * Best practice is to write an entire day's worth of data and store it under the first
 * epochTimeMilliSec for that day.  This might require *you* to do the read-modify-write 
 * if you can't get data from your data source in chunks when there might be > 64k events/year
 *
 */
FsTimeSeriesDB.putEvent = async function(key, event) {
  this.verifyOptions(this.options);
  assert(key, 'key required');
  assert(key.id, 'key.id required');
  assert(key.group1, 'key.group1 required');
  assert(key.group2, 'key.group2 required');
  assert(key.epochTimeMilliSec, 'key.epochTimeMilliSec is required');

  if (event.epochTimeMilliSec === undefined) {
    // epochTimeMilliSec must be at every level of an event heirarchy
    event.epochTimeMilliSec = key.epochTimeMilliSec;
  }
  let marshalledEvent =  { event: event, epochTimeMilliSec: event.epochTimeMilliSec };
  if (key.endTimeMilliSec !== undefined) {
    marshalledEvent.endTimeMilliSec = key.endTimeMilliSec;
  }

  let firstTime = key.epochTimeMilliSec;

  const evtCompressed = await gzip(JSON.stringify(marshalledEvent));
  const firstByte = key.id.substring(0,2);
  const secondByte = key.id.substring(2,4);
  const thirdByte = key.id.substring(4,6);
  const theDate = new Date(key.epochTimeMilliSec);
  const fileName = `${firstTime}.json.gz`;

  const filePath = `${this.options.rootPath}/${theDate.getFullYear()}/${key.group1}/${key.group2}/${firstByte}/${secondByte}/${thirdByte}/${key.id}`;

  //console.log(filePath);
  //console.log(fileName);
  await mkdirp(filePath);
  await fs.writeFile(`${filePath}/${fileName}`, evtCompressed);
  return true;
};


/**
 * @brief get a path to a years worth of data files given a key and a year from Date.getFullYear()
 *
 */
FsTimeSeriesDB.getYearPath = function(key, year) {
  assert(key, 'key required');
  assert(key.id, 'key.id required');
  assert(key.group1, 'key.group1 required');
  assert(key.group2, 'key.group2 required');
  assert(year, 'year of format Javascript Date.getFullYear  is required');

  const firstByte = key.id.substring(0,2);
  const secondByte = key.id.substring(2,4);
  const thirdByte = key.id.substring(4,6);
  const filePath = `${this.options.rootPath}/${year}/${key.group1}/${key.group2}/${firstByte}/${secondByte}/${thirdByte}/${key.id}`;
  return filePath;
};

/**
 *
 */
FsTimeSeriesDB.getEvents = async function(key) {
  this.verifyOptions(this.options);
  assert(key, 'key required');
  assert(key.id, 'key.id required');
  assert(key.group1, 'key.group1 required');
  assert(key.group2, 'key.group2 required');
  assert(key.startTime, 'key.startTime is required');
  assert(key.endTime, 'key.endTime is required');

  const firstByte = key.id.substring(0,2);
  const secondByte = key.id.substring(2,4);
  const thirdByte = key.id.substring(4,6);
  const startDate = new Date(key.startTime);
  const endDate = new Date(key.endTime);

  const firstYear = startDate.getFullYear();
  const secondYear = endDate.getFullYear();
  // @note fix this some time later
  assert(secondYear - firstYear <= 1, 'queries can only span a total of two years');
  let years = [];
  if (firstYear == secondYear) {
    years.push(firstYear);
  } else {
    years.push(firstYear);
    years.push(secondYear);
  }

  // fetch each years events
  let filesAccessed = 0;
  const yearsFilesArray = await Promise.map(years, async (year) => {
    const filePath = this.getYearPath(key, year);
    //console.log(filePath);

    let files = [];;
    try {
      files = await fs.readdir(filePath);
    } catch (e) {
      // directory doesn't exist, so just ignore
    }
    //console.log(files);

    return files.map(el => {
      return {
        filename: el,
        filepath: filePath,
      };
    });
  });

  function getFileTime(filename) {
    return parseInt(filename.substr(0, filename.indexOf('.')));
  };
  function fileTimeCompare(first, second) {
    //console.log(`first ${first}`);
    //console.log(`second ${second}`);
    const firstFileTime = isNaN(first)? getFileTime(first.filename) : first;
    const secondFileTime = isNaN(second)? getFileTime(second.filename) : second;
    return (firstFileTime - secondFileTime);
  };
  const yearsFilesSorted = yearsFilesArray.flat().sort(fileTimeCompare);
  //console.log(yearsFilesSorted);
  let startIndex = bs(yearsFilesSorted, key.startTime, fileTimeCompare);
  let endIndex = bs(yearsFilesSorted, key.endTime, fileTimeCompare);
  // @see https://github.com/darkskyapp/binary-search/issues/1
  startIndex = startIndex < 0? startIndex * -1 - 1 : startIndex; 
  endIndex = endIndex < 0? endIndex * -1 - 1 : endIndex; 

  // heuristic for no files found is if the file time is less than
  // a week old then return  last file as the file might contain
  // data after startTime.  It's a rough heuristic but probably good enough
  if (startIndex >= yearsFilesSorted.length) {
    if (yearsFilesSorted.length > 0 && 
      getFileTime(yearsFilesSorted[yearsFilesSorted.length - 1].filename) + 86400 * 1000 * 7  > key.startTime) {
        startIndex = yearsFilesSorted.length - 1;
    } else { 
      console.log(`requested time span is beyond existing data, returning empty set`);
      return [];
    }
  }
  if (endIndex < yearsFilesSorted.length){
    const endIndexFile = yearsFilesSorted[endIndex];
    const endIndexFileTime = getFileTime(endIndexFile.filename);
    if (endIndexFileTime < key.startTime) {
      console.log(`requested time span is before existing data, returning empty set`);
      return [];
    }
  }
  const files = yearsFilesSorted.slice(startIndex, endIndex + 1); // slice not inclusive of endIndex
  console.log(`indexes are ${startIndex} and ${endIndex}`);
  console.log(`list of files: ${util.inspect(files, false, 4)}`);

  let uncompressedEvents = await Promise.map(files, async (file) => {
    const data = await fs.readFile(`${file.filepath}/${file.filename}`);
    filesAccessed++;
    if (data) {
      const eventUnzipped = await ungzip(data);
      const eventMarshalled = JSON.parse(eventUnzipped);
      if (eventMarshalled.event) {
        return eventMarshalled.event;
      } else {
        return null;
      }
    }
    return null;
  })
    
  uncompressedEvents = uncompressedEvents.filter(el => {
    return (el != null);
  }).flat();
  //console.log(uncompressedEvents);
  console.log(`files accessed: ${filesAccessed}`);
  return uncompressedEvents;
}

/**
 * @brief get the first and last times from the file name
 */
FsTimeSeriesDB.getFileTime = function(filename) {
  const fileTime = filename.substr(0, filename.indexOf('.'));
  const intTime = parseInt(fileTime);
  return intTime;
};

/**
 * @brief removes all data for all years for a key
 *
 * @returns the number of directories deleted
 *
 */
FsTimeSeriesDB.removeData = async function(key) {
  this.verifyOptions(this.options);
  assert(key, 'key required');
  assert(key.id, 'key.id required');
  assert(key.group1, 'key.group1 required');
  assert(key.group2, 'key.group2 required');

  const rootPath= `${this.options.rootPath}/`;
  let yearDirs = await fs.readdir(rootPath);
  
  const removeData = await Promise.map(yearDirs, async (year) => {
    //console.log(`---year ${year}`);
    const filePath = this.getYearPath(key, year);
    //console.log(filePath);

    // https://stackoverflow.com/questions/4482686/check-synchronously-if-file-directory-exists-in-node-js
    // yes they throw for a standard use case.  Ugh.
    let exists = false;
    try {
      await fs.access(filePath);
      exists = true;
    } catch (err) {
      exists = false;
    }

    if (exists) {
      // XXX Requires node ^v12.10.0
      // may have to use @see https://stackoverflow.com/questions/18052762/remove-directory-which-is-not-empty
      console.log(`removing directory ${filePath}`);
      await fs.rmdir(filePath, { 
        recursive: true,
        maxRetries: 3,
        retryDelay: 100,
      });
      return true;
    }
    return false;
  });
  //console.log(removeData);

  const result = removeData.filter(v => v).length;
  //console.log(result);
  return result;
};



/**
 * @brief return the earliest and  the latest time of the latest item in the time series efficiently
 *
 */
FsTimeSeriesDB.getTimeSpan = async function(key) {
  this.verifyOptions(this.options);
  assert(key, 'key required');
  assert(key.id, 'key.id required');
  assert(key.group1, 'key.group1 required');
  assert(key.group2, 'key.group2 required');
  const result = { 
    earliest: Number.MAX_SAFE_INTEGER, 
    latest: Number.MIN_SAFE_INTEGER,
  };

  const rootPath = `${this.options.rootPath}/`;

  let yearDirs = await fs.readdir(rootPath);
  yearDirs = yearDirs.sort();
  //console.log(yearDirs);

  const existsForYears = await Promise.map(yearDirs, async (year) => {
    const filePath = this.getYearPath(key, year);
    //console.log(filePath);

    // https://stackoverflow.com/questions/4482686/check-synchronously-if-file-directory-exists-in-node-js
    // yes they throw for a standard use case.  Ugh.
    let exists = false;
    try {
      await fs.access(filePath);
      exists = true;
    } catch (err) {
      exists = false;
    }
    //console.log(exists);
    return exists;
  });

  //console.log(existsForYears);

  if (existsForYears.findIndex(el => { return el; }) >= 0) {
    //console.log(existsForYears);
    const earliestYearIdx = existsForYears.findIndex(el => { return el; });
    //console.log(earliestYearIdx);
    const latestYearIdx = existsForYears.length - 1 -  existsForYears.reverse().findIndex(el => { return el; });
    //console.log(latestYearIdx);

    const filePathEarliest = this.getYearPath(key, yearDirs[earliestYearIdx]);
    const filePathLatest = this.getYearPath(key, yearDirs[latestYearIdx]);

    const earliestAndLatestPaths = [ filePathEarliest, filePathLatest ];
    //console.log(earliestAndLatestPaths);

    // kind of gross but we have to go in parallel
    const earliestAndLatestFiles = await Promise.map(earliestAndLatestPaths, async (path) => {
      let theseFiles = await fs.readdir(path);
      theseFiles = theseFiles.sort();
      return theseFiles;
    });

    let filenameAndPath;
    if (earliestAndLatestFiles[0].length > 0) {
      result.earliest = this.getFileTime(earliestAndLatestFiles[0][0]);
      filenameAndPath = `${filePathEarliest}/${earliestAndLatestFiles[0][0]}`;
    }
    if (earliestAndLatestFiles[1].length > 0) {
      filenameAndPath = `${filePathLatest}/${earliestAndLatestFiles[1][earliestAndLatestFiles[1].length-1]}`;
    }

    const data = await fs.readFile(filenameAndPath);
    if (data) {
      const eventUnzipped = await ungzip(data);
      const eventMarshalled = JSON.parse(eventUnzipped);
      if (eventMarshalled.endTimeMilliSec !== undefined) {
        result.latest = eventMarshalled.endTimeMilliSec;
      } else {
        // backward compatability / or best effort
        result.latest = this.getFileTime(earliestAndLatestFiles[1][earliestAndLatestFiles[1].length-1]);
      }
    } else {
      // backward compatability / or best effort
      result.latest = this.getFileTime(earliestAndLatestFiles[1][earliestAndLatestFiles[1].length-1]);
    }

  } 
  return result;
 
};


/**
 * @brief return the time of the latest item in the time series efficiently
 *
 */
FsTimeSeriesDB.getLatestTime = async function(key) {

  const span = await this.getTimeSpan(key);
  return span.latest;
 
};

/**
 * @brief return the time of the latest item in the time series efficiently
 *
 */
FsTimeSeriesDB.getEarliestTime = async function(key) {

  const span = await this.getTimeSpan(key);
  return span.earliest;
 
};



'use strict';

const assert = require('assert');
const _ = require('underscore');
const {gzip, ungzip} = require('node-gzip');
const Promise = require('bluebird');
const fs = require('fs').promises;
const mkdirp = require('mkdirp');

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
  assert(options.rootPath, 'root path defined');

  return options;
};

/**
 * Note:  Events are stored as a file that has epochTimeMillisec as the name of the file.
 * This means you can only *put* about 65000 discrete events per day.
 * Best practice is to write an entire day's worth of data and store it under the first
 * epochTimeMillisec for that day.  This might require *you* to do the read-modify-write 
 * if you can't get data from your data source in one day chunks.
 *
 */
FsTimeSeriesDB.putEvent = async function(key, event) {
  this.verifyOptions(this.options);
  assert(key, 'key required');
  assert(key.id, 'key.id required');
  assert(key.group1, 'key.group1 required');
  assert(key.group2, 'key.group2 required');
  assert(key.epochTimeMilliSec, 'key.epochTimeMillisec is required');

  if (event.epochTimeMilliSec === undefined) {
    // epochTimeMillisec must be at every level of an event heirarchy
    event.epochTimeMilliSec = key.epochTimeMilliSec;
  }
  let marshalledEvent =  { event: event, epochTimeMilliSec: event.epochTimeMilliSec };

  let firstTime = event.epochTimeMilliSec;
  let secondTime = firstTime;
  if (_.isArray(event)) {
    // each element of the array must look like
    // a standardized event so that unmarshalling
    // of arrays in getEvents works properly
    const marshalledEvents = event.map(el => {
      if (el.epochTimeMilliSec === undefined) {
        el.epochTimeMilliSec = key.epochTimeMilliSec;
      }
      return el;
    }).sort((first, second) => {
      // the client may not have sorted the array by time, so we will
      return first.epochTimeMilliSec - second.epochTimeMilliSec;
    });
    // marshal same way as solitary event
    marshalledEvent = { event: marshalledEvents, epochTimeMilliSec: key.epochTimeMilliSec }
    secondTime = marshalledEvents[marshalledEvents.length-1].epochTimeMilliSec;
  }

  const evtCompressed = await gzip(JSON.stringify(marshalledEvent));
  const firstByte = key.id.substring(0,2);
  const secondByte = key.id.substring(2,4);
  const thirdByte = key.id.substring(4,6);
  const theDate = new Date(key.epochTimeMilliSec);
  const fileName = `${firstTime}-${secondTime}.json.gz`;

  const filePath = `${this.options.rootPath}/${theDate.getFullYear()}/${key.group1}/${key.group2}/${firstByte}/${secondByte}/${thirdByte}/${key.id}`;

  console.log(filePath);
  console.log(fileName);
  await mkdirp(filePath);
  await fs.writeFile(`${filePath}/${fileName}`, evtCompressed);
  return true;
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
  const yearsUncompressedEvents = await Promise.map(years, async (year) => {
    const filePath = `${this.options.rootPath}/${year}/${key.group1}/${key.group2}/${firstByte}/${secondByte}/${thirdByte}/${key.id}`;

    console.log(filePath);

    const files = await fs.readdir(filePath);
    console.log(files);

    const uncompressedEvents = await Promise.map(files, async (filename) => {
      const fileTimes = filename.substr(0, filename.indexOf('.'));
      const firstTime = parseInt(fileTimes.substr(0, fileTimes.indexOf('-')));
      const lastTime = parseInt(fileTimes.substr(fileTimes.indexOf('-')+1, fileTimes.length-1));
      //console.log(`--firstTime ${firstTime}`);
      //console.log(`--lastTime ${lastTime}`);
      //console.log(`--startTime ${key.startTime}`);
      //console.log(`--endTime ${key.endTime}`);
      if ((firstTime >= key.startTime  && firstTime <= key.endTime && lastTime >= key.startTime) ||  
        (firstTime <= key.startTime && lastTime >= key.startTime)) {
          const data = await fs.readFile(`${filePath}/${filename}`);
          filesAccessed++;
          if (data) {
            const eventUnzipped = await ungzip(data);
            return JSON.parse(eventUnzipped);
          }
      }
      return null;
    }).filter(el => {
      return (el != null);
    });
    //console.log(uncompressedEvents);
    return uncompressedEvents;
  });
  console.log(`---files accessed = ${filesAccessed}`);

  const uncompressedEvents = yearsUncompressedEvents.reduce((acc, el) => {
    // @see https://stackoverflow.com/questions/10865025/merge-flatten-an-array-of-arrays
    return acc.concat(el);
  },[])
  //console.log('---');
  //console.log(uncompressedEvents);
  //console.log('---');

  return uncompressedEvents.reduce((acc, el) => {
    // flatten multiple array results down to one big array
    if (Array.isArray(el.event)) {
      // @see https://stackoverflow.com/questions/10865025/merge-flatten-an-array-of-arrays
      return acc.concat(el.event.filter(innerEl  => {
        // filter out any elements of this array that aren't in the requested time window
        return (innerEl.epochTimeMilliSec >= key.startTime && innerEl.epochTimeMilliSec <= key.endTime);
      }));
    } else {
      acc.push(el.event);
    }
    return acc;
  },[]);
}


/**
 * evts input format: {
 *   EpochTimeMilliSec: <number>
 *   event: <JSON string of event>
 * }
DynamoTimeSeries.putEvents = async function(userId, eventType, evts) {
*/

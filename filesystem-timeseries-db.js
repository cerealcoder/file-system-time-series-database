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
    event.epochTimeMilliSec = key.epochTimeMilliSec;
  }
  let marshalledEvent =  { event: event, epochTimeMilliSec: event.epochTimeMilliSec };

  if (_.isArray(event)) {
    // each element of the array must look like
    // a standardized event so that unmarshalling
    // of arrays in getEvents works properly
    marshalledEvent = event.map(el => {
      if (el.epochTimeMilliSec === undefined) {
        el.epochTimeMilliSec = key.epochTimeMilliSec;
      }
      return el;
    });
  }

  const evtCompressed = await gzip(JSON.stringify(marshalledEvent));
  const firstByte = key.id.substring(0,2);
  const secondByte = key.id.substring(2,4);
  const thirdByte = key.id.substring(4,6);
  const theDate = new Date(key.epochTimeMilliSec);
  const fileName = `${key.epochTimeMilliSec}.json.gz`;

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
  //const fileName = `${key.epochTimeMilliSec}.json.gz`;

  // note this won't work for queries that span years
  const filePath = `${this.options.rootPath}/${startDate.getFullYear()}/${key.group1}/${key.group2}/${firstByte}/${secondByte}/${thirdByte}/${key.id}`;

  console.log(filePath);

  const files = await fs.readdir(filePath);
  console.log(files);

  const uncompressedEvents = await Promise.map(files, async (filename) => {
    const time = filename.substr(0, filename.indexOf('.'));
    if (time >= key.startTime && time < key.endTime) {
      const data = await fs.readFile(`${filePath}/${filename}`);
      if (data) {
        const eventUnzipped = await ungzip(data);
        return JSON.parse(eventUnzipped);
      }
    }
    return null;
  }).filter(el => {
    return (el != null);
  });
  console.log(uncompressedEvents);

  return uncompressedEvents.reduce((acc, el) => {
    // flatten multiple array results down to one big array
    if (Array.isArray(el.event)) {
      Array.prototype.push.apply(acc, el.event)
    } else {
      acc.push(el);
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

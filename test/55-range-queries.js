'use strict'
const assert = require('assert');
const tape = require('tape')
const _test = require('tape-promise').default // <---- notice 'default'
const test = _test(tape) // decorate tape
const Random = require('random-js').Random;
const random = new Random();
const _ = require('underscore');

const FsTimeSeriesDB = require('../filesystem-timeseries-db');
const rootPath = 'test/output';


test('put and get multiple time series events at the same time', async function(t) {
  const dbInstance = Object.create(FsTimeSeriesDB).setOptions({rootPath: rootPath});

  const userId = random.string(16);
  const group1 = 'eventType';
  const group2 = 'signalType';
  const startTime = 100;
  const endTime = 1000;
  const event = { 
    epochTimeMilliSec: startTime,
    eventFoo: 'bar',
  };


  const putResult1 = await dbInstance.putEvent({
    id: userId,
    group1: group1,
    group2, group2,
    epochTimeMilliSec: startTime,
  }, event);
  t.ok(putResult1, 'put result1 is truthy');

  const putResult2 = await dbInstance.putEvent({
    id: userId,
    group1: group1,
    group2, group2,
    epochTimeMilliSec: startTime,
  }, event);
  t.ok(putResult2, 'put result2 is truthy');


  const getResult = await dbInstance.getEvents({
    id: userId,
    group1: group1,
    group2, group2,
    startTime: startTime,
    endTime: endTime,
  });
  //console.log(getResult);
  t.equal(getResult.length, 1, 'two identical item puts, one item queried');
  t.equal(getResult[0].epochTimeMilliSec, startTime, 'event time was the same as was put');
  t.ok(_.isEqual(getResult[0], event), 'event contents queried is same as was put');

});

test('put and get multiple time series events at different times', async function(t) {
  const dbInstance = Object.create(FsTimeSeriesDB).setOptions({rootPath: rootPath});

  const userId = random.string(16);
  const group1 = 'eventType';
  const group2 = 'signalType';
  const startTime = 100;
  const endTime = 1000;
  const event1 = { 
    epochTimeMilliSec: startTime,
    eventFoo: 'bar',
  };
  const event2 = { 
    epochTimeMilliSec: startTime + 1,
    eventFoo: 'bar',
  };


  const putResult1 = await dbInstance.putEvent({
    id: userId,
    group1: group1,
    group2, group2,
    epochTimeMilliSec: event1.epochTimeMilliSec,
  }, event1);
  t.ok(putResult1, 'put result1 is truthy');

  const putResult2 = await dbInstance.putEvent({
    id: userId,
    group1: group1,
    group2, group2,
    epochTimeMilliSec: event2.epochTimeMilliSec,
  }, event2);
  t.ok(putResult2, 'put result2 is truthy');


  const getResult1 = await dbInstance.getEvents({
    id: userId,
    group1: group1,
    group2, group2,
    startTime: startTime,
    endTime: endTime,
  });
  //console.log(getResult1);
  t.equal(getResult1.length, 2, 'two different items put, queried was two items');
  t.equal(getResult1[1].epochTimeMilliSec, startTime +1, 'second event time was the same as was put');
  t.ok(_.isEqual(getResult1[1], event2), 'event contents queried is same as was put');

  const getResult2 = await dbInstance.getEvents({
    id: userId,
    group1: group1,
    group2, group2,
    startTime: startTime,
    endTime: startTime,
  });
  t.equal(getResult2.length, 1, 'two items put, queried one by start time');
  t.equal(getResult2[0].epochTimeMilliSec, startTime , 'event time was the same as startTime');
  t.ok(_.isEqual(getResult2[0], event1), 'event contents queried is same as event2');

  const getResult3 = await dbInstance.getEvents({
    id: userId,
    group1: group1,
    group2, group2,
    startTime: startTime+1,
    endTime: endTime,
  });
  console.log(getResult3);
  t.equal(getResult3.length, 1, 'two items put, queried the very last one');
  t.equal(getResult3[0].epochTimeMilliSec, startTime+1, 'event time was the same as the last event put');
  t.ok(_.isEqual(getResult3[0], event2), 'event contents queried is same as event2');



});

test('Make sure we can put and query arrays of events and query various spans of files', async function(t) {
  const dbInstance = Object.create(FsTimeSeriesDB).setOptions({rootPath: rootPath});

  const userId = random.string(16);
  const group1 = 'eventType';
  const group2 = 'signalType';
  const file1Time = 1000
  const file2Time = 2000
  const events1 = [
    {
      epochTimeMilliSec: file1Time,
      eventFoo: 'first',
    },
    {
      epochTimeMilliSec: file1Time + 1,
      eventFoo: 'second',
    },
    {
      epochTimeMilliSec: file1Time + 2,
      eventFoo: 'third',
    },
  ];

  const events2 = [
    {
      epochTimeMilliSec: file2Time,
      eventFoo: 'fourth',
    },
    {
      epochTimeMilliSec: file2Time + 1,
      eventFoo: 'fifth',
    },
    {
      epochTimeMilliSec: file2Time + 2,
      eventFoo: 'sixth',
    },
  ];

  const putResult1 = await dbInstance.putEvent({
    id: userId,
    group1: group1,
    group2, group2,
    epochTimeMilliSec: events1[0].epochTimeMilliSec,
  }, events1);
  t.ok(putResult1, 'first put result is truthy for an array of events');

  const putResult2 = await dbInstance.putEvent({
    id: userId,
    group1: group1,
    group2, group2,
    epochTimeMilliSec: events2[0].epochTimeMilliSec,
  }, events2);
  t.ok(putResult2, 'second put result is truthy for an array of events');

  const firstFileResult = await dbInstance.getEvents({
    id: userId,
    group1: group1,
    group2, group2,
    startTime: file1Time,       // all elements of the first file
    endTime: file1Time + 2,     // all elements of the first file
  });
  t.equal(firstFileResult.length, 3, 'three events queried, all from the first file');
  t.equal(firstFileResult[2].epochTimeMilliSec, file1Time + 2, 'timestamp for third element is correctly for end of first file');

  const secondFileResult = await dbInstance.getEvents({
    id: userId,
    group1: group1,
    group2, group2,
    startTime: file2Time,       // all elements of the first file
    endTime: file2Time + 2,     // all elements of the first file
  });
  t.equal(secondFileResult.length, 3, 'three events queried, all from the second file');
  t.equal(secondFileResult[2].epochTimeMilliSec, file2Time + 2, 'timestamp for third element is correctly for end of first file');

  const spanFileResult = await dbInstance.getEvents({
    id: userId,
    group1: group1,
    group2, group2,
    startTime: file1Time,   // all elements of the first file
    endTime: file2Time,     // plus the first element of second file
  });
  //console.log(spanFileResult);
  t.equal(spanFileResult.length, 4, 'four events queried, as the time span is inclusive on both ends');
  t.equal(spanFileResult[3].epochTimeMilliSec, file2Time, 'timestamp for fourth element is correctly from middle of file');

  const spanMiddleofFilesResult = await dbInstance.getEvents({
    id: userId,
    group1: group1,
    group2, group2,
    startTime: file1Time + 1,   // the middle and end elements of the first file
    endTime: file2Time + 1,     // plus the first and second element of second file
  });
  //console.log(spanMiddleofFilesResult);
  t.equal(spanMiddleofFilesResult.length, 4, 'four events queried from middle of 2 files');
  t.equal(spanMiddleofFilesResult[0].epochTimeMilliSec, file1Time + 1, 'timestamp for 1st element is correct from middle of file1');
  t.equal(spanMiddleofFilesResult[3].epochTimeMilliSec, file2Time + 1, 'timestamp for 4th element is correct from middle of file2');

});

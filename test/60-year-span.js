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




test('make sure we can query data that spans a year turnover', async function(t) {
  const dbInstance = Object.create(FsTimeSeriesDB).setOptions({rootPath: rootPath});

  const userId = random.string(16);
  const group1 = 'eventType';
  const group2 = 'signalType';
  const file1Time = new Date('Dec 31, 1980').getTime(); 
  const file2Time = new Date('Jan 1, 1981').getTime();
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


  const spanFileResult = await dbInstance.getEvents({
    id: userId,
    group1: group1,
    group2, group2,
    startTime: file1Time,   // all elements of the first file
    endTime: file2Time,     // plus the first element of second file
  });
  console.log(spanFileResult);
  t.equal(spanFileResult.length, 4, 'four events queried, as the time span is inclusive on both ends');
  t.equal(spanFileResult[3].epochTimeMilliSec, file2Time, 'timestamp for fourth element is correctly from middle of file');

  const spanMiddleofFilesResult = await dbInstance.getEvents({
    id: userId,
    group1: group1,
    group2, group2,
    startTime: file1Time + 1,   // the middle and end elements of the first file
    endTime: file2Time + 1,     // plus the first and second element of second file
  });
  console.log(spanMiddleofFilesResult);
  t.equal(spanMiddleofFilesResult.length, 4, 'four events queried from middle of 2 files');
  t.equal(spanMiddleofFilesResult[0].epochTimeMilliSec, file1Time + 1, 'timestamp for 1st element is correct from middle of file1');
  t.equal(spanMiddleofFilesResult[3].epochTimeMilliSec, file2Time + 1, 'timestamp for 4th element is correct from middle of file2');

});

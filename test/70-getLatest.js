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




test('Should be able to return the latest entry for an id', async function(t) {
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

  // make sure we have at least two files in the latest date
  const events3 = [
    {
      epochTimeMilliSec: file2Time + 7,
      eventFoo: 'fourth',
    },
    {
      epochTimeMilliSec: file2Time + 8,
      eventFoo: 'fifth',
    },
    {
      epochTimeMilliSec: file2Time + 9,
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

  const putResult3 = await dbInstance.putEvent({
    id: userId,
    group1: group1,
    group2, group2,
    epochTimeMilliSec: events3[0].epochTimeMilliSec,
  }, events3);
  t.ok(putResult3, 'second put result is truthy for an array of events');


  const latestTime = await dbInstance.getLatestTime({
    id: userId,
    group1: group1,
    group2, group2,
  });
  //console.log(latestTime);
  t.equal(latestTime, file2Time + 9, 'latest Time matches the last entry we put in');

  const timeSpan = await dbInstance.getTimeSpan({
    id: userId,
    group1: group1,
    group2, group2,
  });
  console.log(timeSpan);
  t.equal(timeSpan[0], file1Time, 'earliest Time matches the first entry we put in');
  t.equal(timeSpan[1], file2Time + 9, 'latest Time matches the last entry we put in');


});

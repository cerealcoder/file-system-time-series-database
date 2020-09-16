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


test('put and get a multiple time series events at the same time', async function(t) {
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

test('put and get a multiple time series events at different times', async function(t) {
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
  t.equal(getResult2.length, 1, 'two items put, queried one by time range');
  t.equal(getResult2[0].epochTimeMilliSec, startTime , 'event time was the same as was put');
  t.ok(_.isEqual(getResult2[0], event1), 'event contents queried is same as was put');


});

test('push an array of events and make sure we can query it', async function(t) {
  const dbInstance = Object.create(FsTimeSeriesDB).setOptions({rootPath: rootPath});

  const userId = random.string(16);
  const group1 = 'eventType';
  const group2 = 'signalType';
  const startTime = 100;
  const endTime = 1000;
  const events = [
    {
      epochTimeMilliSec: startTime,
      eventFoo: 'first',
    },
    {
      epochTimeMilliSec: startTime + 1,
      eventFoo: 'second',
    },
    {
      epochTimeMilliSec: startTime + 2,
      eventFoo: 'third',
    },
  ];


  const putResult = await dbInstance.putEvent({
    id: userId,
    group1: group1,
    group2, group2,
    epochTimeMilliSec: events[0].epochTimeMilliSec,
  }, events);
  t.ok(putResult, 'put result is truthy for an array of events');

  const getResult = await dbInstance.getEvents({
    id: userId,
    group1: group1,
    group2, group2,
    startTime: startTime,
    endTime: endTime,
  });
  //console.log(getResult);
  t.equal(getResult.length, 3, 'three events queried, just like what was in the array');
  t.equal(getResult[1].epochTimeMilliSec, startTime +1 , 'second event time was the same as was put');
  t.ok(_.isEqual(getResult[2], events[2]), 'event contents queried is same as was put');


});

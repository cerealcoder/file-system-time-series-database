'use strict'
const assert = require('assert');
const tape = require('tape')
const _test = require('tape-promise').default // <---- notice 'default'
const test = _test(tape) // decorate tape
const Random = require('random-js').Random;
const random = new Random();
const _ = require('underscore');
const Promise = require('bluebird');

const FsTimeSeriesDB = require('../filesystem-timeseries-db');
const rootPath = 'test/output';


test('Put data and query at the end', async function(t) {
  const dbInstance = Object.create(FsTimeSeriesDB).setOptions({rootPath: rootPath});

  const userId = random.string(16);
  const group1 = 'eventType';
  const group2 = 'signalType';
  const startTime = 100;
  const endTime = 1000;
  const event1 = { 
    epochTimeMilliSec: startTime,
    eventFoo: 'first',
  };
  const event2 = { 
    epochTimeMilliSec: startTime + 100,
    eventFoo: 'second',
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
    startTime: startTime + 110,
    endTime: endTime,
  });
  //console.log(getResult1);
  t.equal(getResult1.length, 1, 'two different items put, queried after end got 1 item');
  t.equal(getResult1[0].epochTimeMilliSec, event2.epochTimeMilliSec, 'second event time was the same as was put');
  t.ok(_.isEqual(getResult1[0], event2), 'event contents queried is same as was put');

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
  t.equal(getResult3.length, 2, 'two items put, queried in middle returns both');
  t.equal(getResult3[0].epochTimeMilliSec, event1.epochTimeMilliSec, 'event time was the same as the last event put');
  t.ok(_.isEqual(getResult3[0], event1), 'event contents queried is same as event2');

});


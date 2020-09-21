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


function delay (time) {
  return new Promise(function (resolve, reject) {
    setTimeout(function () {
      resolve()
    }, time)
  })
}
 
const rootPath = 'test/output';

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



  const putResults = await Promise.map(events, async (el) => {
    const putResult = await dbInstance.putEvent({
      id: userId,
      group1: group1,
      group2, group2,
      epochTimeMilliSec: el.epochTimeMilliSec,
    }, el);
    t.ok(putResult, 'put result is truthy for an an element of a list of events');
  });

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

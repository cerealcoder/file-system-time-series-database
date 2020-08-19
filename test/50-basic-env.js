'use strict'
const assert = require('assert');
const tape = require('tape')
const _test = require('tape-promise').default // <---- notice 'default'
const test = _test(tape) // decorate tape
const Random = require('random-js').Random;
const random = new Random();
const _ = require('underscore');

const FsTimeSeriesDB = require('../filesystem-timeseries-db');


function delay (time) {
  return new Promise(function (resolve, reject) {
    setTimeout(function () {
      resolve()
    }, time)
  })
}
 
const rootPath = 'test/output';

test('put and get a time series event', async function(t) {
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


  const putResult = await dbInstance.putEvent({
    id: userId,
    group1: group1,
    group2, group2,
    epochTimeMilliSec: startTime,
  }, event);
  console.log(putResult);
  t.ok(putResult, 'put result is truthy');

  const getResult = await dbInstance.getEvents({
    id: userId,
    group1: group1,
    group2, group2,
    startTime: startTime,
    endTime: endTime,
  });
  //console.log(getResult);
  t.equal(getResult.length, 1, 'one item put, one item queried');
  t.equal(getResult[0].epochTimeMilliSec, startTime, 'event time was the same as was put');
  t.ok(_.isEqual(getResult[0].event, event), 'event contents queried is same as was put');

});

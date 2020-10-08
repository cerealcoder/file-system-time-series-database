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

test('query a time series event with the wrong year, which used to cause an exception', async function(t) {
  const dbInstance = Object.create(FsTimeSeriesDB).setOptions({rootPath: rootPath});

  const userId = random.string(16);
  const group1 = 'eventType';
  const group2 = 'signalType';
  const startTime = 100;
  const endTime = 1000;
  const queryTime = new Date('1-1-2020').getTime();
  const event = { 
    epochTimeMilliSec: startTime,
    eventFoo: 'bar',
  };


  const putResult = await dbInstance.putEvent({
    id: userId,
    group1: group1,
    group2, group2,
    epochTimeMilliSec: queryTime,
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
  console.log(getResult);
  t.equal(getResult.length, 0, 'no items should be returned because the year is wrong');

});


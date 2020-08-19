'use strict'
var tape = require('tape')
var _test = require('tape-promise').default // <---- notice 'default'
var test = _test(tape) // decorate tape

// example function that returns a Promise
// it could also be an async function
function delay (time) {
  return new Promise(function (resolve, reject) {
    setTimeout(function () {
      resolve()
    }, time)
  })
}
 
// NOTICE 'async'?
test('ensure async works', async function (t) {
  await delay(100)
  t.true(true, 'if you got this far, you delayed 100ms successfully')
});

/*
 * @see https://github.com/substack/tape/issues/512
 *
 */
test('ensure JSON.parse can be tested to not throw', async function(t) {
  t.doesNotThrow(function() { JSON.parse('{}') }, undefined, 'JSON.parse should not throw on an empty brackets');
});

test('ensure JSON.parse can be tested to throw', async function(t) {
  t.throws(function () { JSON.parse('') }, undefined, 'JSON.parse should throw on an empty string');
});

test('ensure async function  can be tested to throw', async function(t) {
  // t.throw works synchronously
  function normalThrower() {
    throw(new Error('an artificial synchronous error'));
  };
  t.throws(function () { normalThrower() }, /artificial/, 'should be able to test that a normal function throws an artificial error');

  // you have to do this for async functions, you can't just throw in async into t.throws
  async function asyncThrower() {
    throw(new Error('an artificial asynchronous error'));
  };
  try {
    await asyncThrower();
    t.fail('async thrower did not throw');
  } catch (e) {
    t.match(e.message,/asynchronous/, 'asynchronous error was thrown');
  };
});

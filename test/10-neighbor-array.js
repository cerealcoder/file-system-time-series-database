'use strict'
const tape = require('tape')
const _test = require('tape-promise').default // <---- notice 'default'
const test = _test(tape) // decorate tape

const bs = require('binary-search');
 
test('merge neighbors in a sorted array', async function (t) {
  const testArray = [
    100,
    200,
    300,
    400,
    500,
    700,   // unsorted on purpose
    600,
    800,
    950,  // non-monotonic increase on purpose
    1000,
    2000,
  ];

  const neighborArray = testArray.sort((firstEl, secondEl) => { 
    // @NOTE that Javascript does string comparisons by default, so for numeric sort must do this
    return firstEl - secondEl 
  }).reduce((acc, el, idx, array) => {
    if (idx === (array.length - 1)) {
      acc.push([el,Number.MAX_SAFE_INTEGER]);
    } else {
      console.log(`accumulator: ${acc}`);
      console.log(`el: ${el}`);
      console.log(`idx: ${idx}`);
      acc.push([array[idx],array[idx+1]]);
    }
    return acc;
  },[]);
  console.log(neighborArray);

  const result = bs(neighborArray, 600, (el, needle) => {
    return el[0] - needle[0];
  });
  console.log(result);
});

test('find neighbors in an array', async function (t) {
  const testArray = [
    100,
    200,
    300,
    400,
    500,
    700,   // unsorted on purpose
    600,
    800,
    950,   // non-monotonic increase on purpose
    1000,
    2000,
  ];

  const sortedArray = testArray.sort((firstEl, secondEl) => { 
    // @NOTE that Javascript does string comparisons by default, so for numeric sort must do this
    return firstEl - secondEl 
  });
  console.log(sortedArray);

  let result = bs(sortedArray, 650, (el, needle) => {
    return el - needle;
  });
  // @see https://github.com/darkskyapp/binary-search/issues/1
  let absresult = result < 0? result * -1 -1: result;
  console.log(`index for element is ${absresult}`);
  console.log(sortedArray[absresult]);
  t.equal(sortedArray[absresult], 700, '700 is the next number after 650');

  result = bs(sortedArray, 600, (el, needle) => {
    return el - needle;
  });
  // @see https://github.com/darkskyapp/binary-search/issues/1
  absresult = result < 0? result * -1 - 1: result;
  console.log(`index for element is ${absresult}`);
  console.log(sortedArray[absresult]);
  t.equal(sortedArray[absresult], 600, '600 is the next number after 600');

  // prior to
  result = bs(sortedArray, 99, (el, needle) => {
    return el - needle;
  });
  // @see https://github.com/darkskyapp/binary-search/issues/1
  absresult = result < 0? result * -1 - 1: result;
  console.log(`index for element is ${absresult}`);
  console.log(sortedArray[absresult]);
  t.equal(sortedArray[absresult], 100, '100 is the next number after 600');

  // after
  result = bs(sortedArray, 19999, (el, needle) => {
    return el - needle;
  });
  // @see https://github.com/darkskyapp/binary-search/issues/1
  absresult = result < 0? result * -1 - 1: result;
  console.log(`index for element is ${absresult}`);
  console.log(sortedArray[absresult]);
  t.equal(absresult, testArray.length, 'the index is the length of the test array when value is larger than any in array');



});

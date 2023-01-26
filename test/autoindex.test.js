
const AutoIndex = require('../source/autoindex')
const fs = require('fs')
var path = require('path');

// import tap test
const tap = require('tap')

// expected results
const expected = require('./data/test_autoindex_results')

// data
const inputDir = "./test/data"

// temporary dir
const tmp_dir = "./autoindex_test_output_tmp"

// test initialization
tap.before(async function() { 
  fs.mkdirSync(tmp_dir)
})

tap.test('get indexes from queries in directory default options', async (childTest) => {
    let autoindex = new AutoIndex()
    let result = autoindex.autoindex(inputDir) 
    childTest.equal(JSON.stringify(expected.expectedResultMinimizeFalse), JSON.stringify(result))
})

tap.test('get indexes from queries in directory minimized and written to disk', async (childTest) => {
    let autoindex = new AutoIndex()
    let result = autoindex.autoindex(inputDir, outputDir=tmp_dir, minimize=true) 
    childTest.equal(JSON.stringify(expected.expectedResultMinimizeTrue), JSON.stringify(result))

    // check output file
    const dir = fs.opendirSync(tmp_dir)
    let dirent
    while ((dirent = dir.readSync()) !== null) {
        if (dirent.name.endsWith(".json")) {
            console.log(dirent.name) 
            let resultJsonString = fs.readFileSync(path.join( tmp_dir, dirent.name), 'utf8');
            let resultJson = JSON.parse(resultJsonString)
            childTest.equal(JSON.stringify(expected.expectedResultExportMinimizeTrue), JSON.stringify(resultJson))
        }
        break
    }
})

tap.test('get indexes from queries in directory minimized and written to disk not verbosed', async (childTest) => {
    let autoindex = new AutoIndex()
    let result = autoindex.autoindex(inputDir, outputDir=tmp_dir, minimize=true, verbose=false) 
    childTest.equal(JSON.stringify(expected.expectedResultMinimizeTrue), JSON.stringify(result))
})

// test cleanup
tap.teardown(async function() { 
    fs.rmSync(tmp_dir, { recursive: true, force: true });
})
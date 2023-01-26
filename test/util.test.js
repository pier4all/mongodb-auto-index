const util = require('../source/util')
const Index = require("../source/index")
const fs = require('fs')


// import tap test
const tap = require('tap')

// data
const inputDir = "./test/data"

//  temporary dir
const tmp_dir = "./util_test_output_tmp"

// test initialization
tap.before(async function() { 
  fs.mkdirSync(tmp_dir)
})

tap.test('get aggregations from directory', async (childTest) => {
  const defaultCollection  = "default_collection"
  const aggregations = util.getAggregationsFromDir(inputDir, defaultCollection)

  let aggregationDefaultCollection = aggregations.find(element => element.name === "test-aggregation-no-coll");
  let aggregationWithCollection = aggregations.find(element => element.name === "test-aggregation-from-file");
  let aggregationFromQuery = aggregations.find(element => element.name === "test-query-01");
  
  childTest.equal(aggregations.length, 4)
  childTest.equal(aggregationDefaultCollection.collection, defaultCollection)
  childTest.equal(aggregationWithCollection.collection, "test_collection")
  childTest.equal(aggregationFromQuery.pipeline.length, 2)
  childTest.end()
})

tap.test('get aggregations from directory without default collection', async (childTest) => {
  const aggregations = util.getAggregationsFromDir(inputDir)

  let aggregationDefaultCollection = aggregations.find(element => element.name === "test-aggregation-no-coll");
  let aggregationWithCollection = aggregations.find(element => element.name === "test-aggregation-from-file");

  childTest.equal(aggregationDefaultCollection.collection, undefined)
  childTest.equal(aggregationWithCollection.collection, "test_collection")
  childTest.equal(aggregations.length, 4)
  childTest.end()
})

tap.test('get aggregationstage operator', async (childTest) => {
  const stage = {"$match": {"name": "Fran"}, "bad_element": "bad"}
  const operator = util.getAggregationStageOperator(stage)

  childTest.equal(operator, "$match")
  childTest.end()
})

tap.test('remove existing object from array by matching key-value pair', async (childTest) => {
  const object_array = [
     {"key":{"color":1},"name":"custom-01_figures_match_0"},
     {"key":{"shape":1},"name":"custom-02_figures_sort_1"},
     {"key":{"size":1},"name":"custom-03_figures_sort_2"},
     {"key":{"round":1},"name":"custom-04_figures_match_3"}
  ]
  const key2delete = "name"
  const value2delete = "custom-03_figures_sort_2"
  const new_array = util.removeIndexFromArray(object_array, key2delete, value2delete)
  childTest.equal(object_array.length-1, new_array.length)
  childTest.end()
})

tap.test('remove non-existing object from array by matching key-value pair', async (childTest) => {
  const object_array = [
    {"key":{"color":1},"name":"custom-01_figures_match_0"},
    {"key":{"shape":1},"name":"custom-02_figures_sort_1"},
    {"key":{"round":1},"name":"custom-04_figures_match_3"}
 ]
  const key2delete = "name"
  const value2delete = "custom-03_figures_sort_2"
  const new_array = util.removeIndexFromArray(object_array, key2delete, value2delete)
  childTest.equal(object_array.length, new_array.length)
  childTest.end()
})

tap.test('write indexes to file', async (childTest) => {
  
  const object_array = [
    Index.fromJSON(JSON.stringify({"name":"test_index2","key":{"name":1},"collection":"test_collection2","operator": '$match', "order": 0, "options":{}})),
    Index.fromJSON(JSON.stringify({"name":"test_index2a","key":{"name":1,"active":1},"collection":"test_collection2","operator": '$match', "order": 0, "options":{}})),
    Index.fromJSON(JSON.stringify({"name":"test_index2b","key":{"name":1,"address":1},"collection":"test_collection2","operator": '$match', "order": 0, "options":{}})) 
  ]
  const result_array = util.writeIndexResults(object_array, tmp_dir)
  childTest.equal(object_array.length, result_array.length)
  childTest.equal(JSON.stringify(object_array[0].toMongoJSON()), JSON.stringify(result_array[0]))

  let filesFound = fs.readdirSync(tmp_dir)
  childTest.equal(filesFound.length, 1)

  let contents = JSON.parse(fs.readFileSync(tmp_dir + '/' + filesFound[0]))
  childTest.equal(JSON.stringify(object_array[0].toMongoJSON()), JSON.stringify(contents.indexes[0]))

  childTest.end()
})

// test cleanup
tap.teardown(async function() { 
  fs.rmSync(tmp_dir, { recursive: true, force: true });
})
  
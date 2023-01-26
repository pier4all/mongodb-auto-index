const Aggregation = require('../source/aggregation')
const util = require('../source/util')
const Generator = require('../source/generator')

// import tap test
const tap = require('tap')

// data
const jsonAgg1 = '{ "aggregate": "test1", "collection": "test_collection", "pipeline": [{"$match": {"age": 35}}, {"$sort": {"salary": 1}}], "allowDiskUse":true}'
const jsonAgg2 = '{ "aggregate": "test2", "collection": "test_collection", "pipeline": [{"$match": {"name": "Karl", "active": false, "$unknown_op": 0}}], "allowDiskUse":true}'
const jsonAggNoIndex = '{ "aggregate": "test_no_index1", "collection": "test_collection", "pipeline": [{"$unwind": "$adresses"}, {"$match": {"zip": 16543}}], "allowDiskUse":true}'
const jsonAggNoIndex2 = '{ "aggregate": "test_no_index2", "collection": "test_collection", "pipeline": [{"$sort": {"salary": {"$unknown_op": 0}}}, {"$sort": {"$unknown_op": 0}}, {"$match": {"$unknown_op": 0}}], "allowDiskUse":true}'
const jsonAggLogic = '{ "aggregate": "test_logic", "collection": "test_collection", "pipeline": [{"$sort": {"salary": 1}}, {"$match": {"$or": [{"age": 30}, {"$and": [{"age": 35}, {"vip": true}, {"$or": [{"$unknown_op": 3}]}]}, {"$unknown_op": {}}]}}]}'
const jsonAggSort = '{ "aggregate": "test_sort1", "collection": "test_collection", "pipeline": [{"$sort": {"salary": -1}}, {"$group": {"_id": { "x" : "$x" },"y": { "$first" : "$y" }}}, {"$sort": {"age": 1}}]}'
const jsonAggGroup1 = '{ "aggregate": "test_group1", "collection": "test_collection", "pipeline": [{"$group": {"_id": { "x" : "$x" },"y": { "$first" : "$y" }}}, {"$group": {"_id": { "z" : "$z" },"k": { "$first" : "$k" }}}]}'
const jsonAggGroup2 = '{ "aggregate": "test_group2", "collection": "test_collection", "pipeline": [{"$match": {"z": 35}}, {"$sort": { "x" : 1, "y": 1}}, {"$group": {"_id": { "x" : "$x" },"y": { "$first" : "$y" }}}]}'
const jsonAggGroup3 = '{ "aggregate": "test_group3", "collection": "test_collection", "pipeline": [{"$unwind": "$t"}, {"$sort": { "x" : 1, "y": 1}}, {"$group": {"_id": { "x" : "$x" },"y": { "$first" : "$y" }}}]}'
const jsonAggGroup4 = '{ "aggregate": "test_group4", "collection": "test_collection", "pipeline": [{"$sort": { "x" : 1, "y": 1}}, {"$group": {"_id": { "x" : "$x", "z": "$z" },"y": { "$first" : "$y" }}}]}'
const jsonAggLookup1 = JSON.stringify({ "aggregate": "test_lookup1", "collection": "test_collection", 
                                        "pipeline": [{"$lookup": { "from" : "foreign_coll", "localField" : "local_field", "foreignField" : "foreign_field", "as" : "new_field"}},
                                                     {"$lookup": { "from" : "foreign_coll", "localField" : "local_field2", "foreignField" : "_id", "as" : "new_field_id"}}]})
const jsonAggLookup2 = JSON.stringify({ "aggregate": "test_lookup2", "collection": "test_collection", 
                                        "pipeline": [ {"$lookup": {"from" : "foreign_coll", "let" : {"local": "$local_field"}, 
                                                                   "pipeline" : [{ "$match": { "$expr": { "$eq": ["$$ref", "$empno"]}}}],
                                                                   "as" : "new_field"}},
                                                      {"$lookup": {"from" : "foreign_coll", "let" : {"local": "$local_field"}, 
                                                                   "pipeline" : [
                                                                    {"$lookup": {"from": "orders-lineitem", "localField": "_id", "foreignField": "o_custkey", "as": "c_orders"}}, {"$match": {"foreign_field_2": 0}}
                                                                   ],
                                                                   "as" : "new_field"}}]})

const jsonMatchLogicalOp = JSON.stringify({ "aggregate": "test_logical_op", "collection": "test_collection", "pipeline": [
                                                                    {"$match": { "$expr": {
                                                                          "$not": [ {
                                                                              "$and": [ { "$lt": [ "$start", "{$END_DATE}" ] }, { "$gt": [ "$end", "{$START_DATE}" ] }
                                                                              ]
                                                                            }
                                                                          ]}}}]})
const jsonMatchSortAliases = JSON.stringify({ "aggregate": "test1", "collection": "test_collection", "pipeline": [{"$addFields": {"age": "$o.age", "salary": "$o.payment"}}, {"$match": {"age": 35}}, {"$sort": {"salary": 1, "x": -1}}], "allowDiskUse":true})
const jsonAggGroupAliases1 = JSON.stringify({ "aggregate": "test_group_alias1", "collection": "test_collection", "pipeline": [{"$addFields": {"age": "$o.age"}}, {"$group": {"_id": { "x" : "$age" },"y": { "$first" : "$y" }}}]})
const jsonAggGroupAliases2 = JSON.stringify({ "aggregate": "test_group_alias2", "collection": "test_collection", "pipeline": [{"$addFields": {"age": "$o.age"}}, {"$sort": { "x" : 1, "age": 1}}, {"$group": {"_id": { "x" : "$x" },"y": { "$first" : "$age" }}}]})
const jsonAggGroupAliases3 = JSON.stringify({ "aggregate": "test_group_alias3", "collection": "test_collection", "pipeline": [{"$addFields": {"age": "$o.age"}}, {"$sort": { "x" : 1, "age": 1}}, {"$group": {"_id": { "x" : "$age" },"y": { "$first" : "$y" }}}]})
const jsonAggGroupAliases4 = JSON.stringify({ "aggregate": "test_group_alias3", "collection": "test_collection", "pipeline": [{"$addFields": {"age": "$o.age"}}, {"$group": {"_id": { "x" : "$x" },"y": { "$first" : "$age" }}}]})
const jsonMatchMultipleAliases1 = JSON.stringify({ "aggregate": "test_mult_aliases", "collection": "test_collection", "pipeline": [{"$addFields": {"salary": 10}}, {"$addFields": {"age": "$o.age"}}, {"$addFields": {"years": "$age"}}, {"$match": {"years": 35, "salary": 5 }}], "allowDiskUse":true})
const jsonMatchMultipleAliases2 = JSON.stringify({ "aggregate": "test_mult_aliases", "collection": "test_collection", "pipeline": [{"$project": {"salary": "$payment"}}, {"$project": {"age": "$o.age", "billed": 0}}, {"$addFields": {"years": "$age"}}, {"$match": {"years": 35, "salary": 5 }}], "allowDiskUse":true})
const jsonMatchPartialAliases = JSON.stringify({ "aggregate": "test_part_aliases", "collection": "test_collection", "pipeline": [{ "$project" :{ "lines": "$edges", "color":1 } }, { "$addFields" :{ "color": "none",  "numedges": "$lines.count" } }, { "$match":{ "lines.size": 5, "numedges": {"$gt": 2},  "color": "none" }}], "allowDiskUse":true})

const jsonComplexExpr = JSON.stringify({ "aggregate": "test_expr", "collection": "test_collection", "pipeline": [{ "$match":{"$expr": {"$eq": ["$username", "lucia.espona@fhnw.ch"]}}}], "allowDiskUse":true})
const jsonLogicalComplexExpr = JSON.stringify({ "aggregate": "test_logical_expr", "collection": "test_collection", "pipeline": [{ "$match":{ "$and": [{ "$expr": {"$eq": ["$employee", "60a17c017c75ab7cd56da552"]}},{"$expr": {"$gte": ["$date", {"$toDate": "2020-07-13T00:00:00.000+00:00"}]}}]}}], "allowDiskUse":true})

// test initialization
tap.before(async function() { 

})

tap.test('generate indexes for aggregation list', async (childTest) => {
  const aggOne = Aggregation.fromJSON(jsonAgg1)
  const aggTwo = Aggregation.fromJSON(jsonAgg2)
  const aggNoIndex = Aggregation.fromJSON(jsonAggNoIndex)
  const aggNoIndex2 = Aggregation.fromJSON(jsonAggNoIndex2)
  
  const generator = new Generator()

  const result = generator.generate([aggOne, aggTwo, aggNoIndex, aggNoIndex2])

  childTest.equal(result["test1"].length, 2)
  childTest.equal(result["test2"].length, 1)
  childTest.equal(result["test_no_index1"].length, 0)
  childTest.equal(result["test_no_index2"].length, 0)

  childTest.end()
})

tap.test('generate indexes for match stage', async (childTest) => {
  const aggregation = Aggregation.fromJSON(jsonAgg2)
  const matchStage = aggregation.pipeline[0]
  const sequence = aggregation.pipeline.map(util.getAggregationStageOperator)
  
  const generator = new Generator()

  const index = generator.getMatchIndex("$match", matchStage, 0, sequence, aggregation, "test_index")

  const expectedIndex = {"name":"test_index","key":{"name":1,"active":1},"collection":"test_collection","operator": '$match', "order": 0, "options":{}} 
  childTest.equal(JSON.stringify(index), JSON.stringify(expectedIndex))

  childTest.end()
})

tap.test('generate indexes for not applicable match stage', async (childTest) => {
  const aggregation = Aggregation.fromJSON(jsonAggNoIndex)
  const matchStage = aggregation.pipeline[1]
  const sequence = aggregation.pipeline.map(util.getAggregationStageOperator)
  
  const generator = new Generator()

  const index = generator.getMatchIndex("$match", matchStage, 1, sequence, aggregation, "test_index")

  childTest.equal(index, undefined)
  childTest.end()
})

tap.test('generate indexes for match stage with logic operations', async (childTest) => {
  const aggregation = Aggregation.fromJSON(jsonAggLogic)
  const matchStage = aggregation.pipeline[1]
  const sequence = aggregation.pipeline.map(util.getAggregationStageOperator)
  
  const generator = new Generator()

  const index = generator.getMatchIndex("$match", matchStage, 1, sequence, aggregation, "test_index_logic")

  const expectedIndex = {"name":"test_index_logic","key":{"age": 1, "vip": 1},"collection":"test_collection","operator": '$match', "order": 1,"options":{}} 

  childTest.equal(JSON.stringify(index), JSON.stringify(expectedIndex))
  childTest.end()
})

tap.test('generate indexes for match stage with comparison expression', async (childTest) => {
  const aggregation = Aggregation.fromJSON(jsonAggLogic)
  const generator = new Generator()

  // less sort fields than in the group compound fields
  aggregation.pipeline = [{"$match":{"$expr": { "$eq" : ["admin", "$username"]}}}, {"$match":{"$expr": { "$unknown_op" : ["test", "$test"]}}}]
  const indexes1 = generator.generateIndexes(aggregation)
  childTest.equal(indexes1.length, 1)
  childTest.equal(indexes1[0].operator, "$match")

  const expectedIndex = {"name":"test_logic_test_collection_match_0","key":{"username": 1,},"collection":"test_collection","operator": '$match', "order": 0,"options":{}} 

  childTest.equal(JSON.stringify(indexes1[0]), JSON.stringify(expectedIndex))
  childTest.end()
})

tap.test('generate index for valid sort stage', async (childTest) => {
  const aggregation = Aggregation.fromJSON(jsonAggSort)
  const order = 0
  const stage = aggregation.pipeline[order]
  const sequence = aggregation.pipeline.map(util.getAggregationStageOperator)
  
  const generator = new Generator()

  const index = generator.getSortIndex("$sort", stage, order, sequence, aggregation, "test_index")

  const expectedIndex = {"name":"test_index", "key":{"salary": -1}, "collection":"test_collection","operator": '$sort', "order":order, "options":{}} 

  childTest.equal(JSON.stringify(index), JSON.stringify(expectedIndex))
  childTest.end()
})

tap.test('generate no index for sort stage after group', async (childTest) => {
  const aggregation = Aggregation.fromJSON(jsonAggSort)
  const order = 2
  const stage = aggregation.pipeline[order]
  const sequence = aggregation.pipeline.map(util.getAggregationStageOperator)
  
  const generator = new Generator()

  const index = generator.getSortIndex("$sort", stage, order, sequence, aggregation, "test_index")

  childTest.equal(index, undefined)
  childTest.end()
})

tap.test('generate index for group stages with no previous sort', async (childTest) => {
  const aggregation = Aggregation.fromJSON(jsonAggGroup1)
   
  const generator = new Generator()

  const indexes = generator.generateIndexes(aggregation)

  childTest.equal(indexes.length, 1)

  const expectedIndex = {"name":"test_group1_test_collection_group_0", "key":{"x": 1, "y": 1}, "collection":"test_collection", "operator": '$group', "order": 0, "options":{}} 
  childTest.equal(JSON.stringify(indexes[0]), JSON.stringify(expectedIndex))

  childTest.end()
})

tap.test('generate index for group stage with previous suitable sort', async (childTest) => {
  const aggregation = Aggregation.fromJSON(jsonAggGroup2)
  const order = 2
  const stage = aggregation.pipeline[order]
  const sequence = aggregation.pipeline.map(util.getAggregationStageOperator)
  
  const generator = new Generator()

  const index = generator.getGroupIndex("$group", stage, order, sequence, aggregation, "test_index")

  const expectedIndex = {"name":"test_index", "key":{"x": 1, "y": 1}, "collection":"test_collection", "operator": '$group', "order": order, "options":{}} 
  childTest.equal(JSON.stringify(index), JSON.stringify(expectedIndex))

  childTest.end()
})

tap.test('generate no index for group stages with previous unwind', async (childTest) => {
  const aggregation = Aggregation.fromJSON(jsonAggGroup3)
   
  const generator = new Generator()

  const indexes = generator.generateIndexes(aggregation)

  // returns one sort index
  childTest.equal(indexes.length, 0)
  childTest.end()
})

tap.test('generate no index for group stages with no suitable previous sort', async (childTest) => {
  const aggregation = Aggregation.fromJSON(jsonAggGroup4)
   
  const generator = new Generator()

  const indexes = generator.generateIndexes(aggregation)

  childTest.equal(indexes.length, 1)
  childTest.equal(indexes[0].operator, "$sort")
  childTest.end()
})

tap.test('group stage corner cases', async (childTest) => {
  const jsonAggGroupCorner = '{ "aggregate": "test_group_corner", "collection": "test_collection", "pipeline": []}'
  const aggregation = Aggregation.fromJSON(jsonAggGroupCorner)
  const generator = new Generator()

  // less sort fields than in the group compound fields
  aggregation.pipeline = [{"$sort": { "x" : 1}}, {"$group": {"_id": { "x" : "$x", "y": "$y" },"z": { "$first" : "$z" }}}]
  const indexes1 = generator.generateIndexes(aggregation)
  childTest.equal(indexes1.length, 2)
  childTest.equal(indexes1[0].operator, "$sort")
  childTest.equal(indexes1[1].operator, "$group")
  
  const expectedIndex1 = {"name":"test_group_corner_test_collection_group_1", "key":{"x": 1, "y": 1, "z": 1}, "collection":"test_collection", "operator": '$group', "order": 1, "options":{}} 
  childTest.equal(JSON.stringify(indexes1[1]), JSON.stringify(expectedIndex1))

  // not valid operator in the group compound fields
  aggregation.pipeline = [{"$sort": { "x" : 1, "y": 1}}, {"$group": {"_id": { "x" : "$x"},"y": { "$sum" : "$y" }}}]
  const indexes2 = generator.generateIndexes(aggregation)
  childTest.equal(indexes2.length, 2)
  childTest.equal(indexes2[0].operator, "$sort")
  childTest.equal(indexes2[1].operator, "$group")
  
  const expectedIndex2 = {"name":"test_group_corner_test_collection_group_1", "key":{"x": 1}, "collection":"test_collection", "operator": '$group', "order": 1, "options":{}} 
  childTest.equal(JSON.stringify(indexes2[1]), JSON.stringify(expectedIndex2))
  
  // group with compound _id and first in pipeline
  aggregation.pipeline = [{"$group": {"_id": { "x" : "$x", "y": "$y" },"z": { "$first" : "$z" }}}]
  const indexes3 = generator.generateIndexes(aggregation)
  childTest.equal(indexes3.length, 0)

  // group first in pipeline with an operator that is not $first
  aggregation.pipeline = [{"$group": {"_id": { "x" : "$x"},"z": { "$sum" : "$z" }}}]
  const indexes4 = generator.generateIndexes(aggregation)
  childTest.equal(indexes4.length, 0)
  
  childTest.end()
})

tap.test('generate index for basic lookup stage but not if foreign field is _id', async (childTest) => {
  const aggregation = Aggregation.fromJSON(jsonAggLookup1)
   
  const generator = new Generator()

  const indexes = generator.generateIndexes(aggregation)

  childTest.equal(indexes.length, 1)
  childTest.equal(indexes[0].operator, "$lookup")
  
  const expectedIndex = {"name":"test_lookup1_test_collection_lookup_0","key":{"foreign_field":1},"collection":"foreign_coll","operator":"$lookup","order":0,"options":{}}
  childTest.equal(JSON.stringify(indexes[0]), JSON.stringify(expectedIndex))

  childTest.end()
})

tap.test('generate index for lookup stage with pipeline', async (childTest) => {
  const aggregation = Aggregation.fromJSON(jsonAggLookup2)
      
  const generator = new Generator()
   
  const indexes = generator.generateIndexes(aggregation)
   
  childTest.equal(indexes.length, 1)
 
  const expectedIndex = {"name":"test_lookup2_test_collection_lookup_0","key":{"empno":1},"collection":"foreign_coll","operator":"$lookup","order":0,"options":{}}
  childTest.equal(JSON.stringify(indexes[0]), JSON.stringify(expectedIndex))
   
  childTest.end()
})

tap.test('generate index for match with logical operator', async (childTest) => {
  const aggregation = Aggregation.fromJSON(jsonMatchLogicalOp)
   
  const generator = new Generator()

  const indexes = generator.generateIndexes(aggregation)

  childTest.equal(indexes.length, 1)
  childTest.equal(indexes[0].operator, "$match")
  
  const expectedIndex = {"name":"test_logical_op_test_collection_match_0","key":{"start":1,"end":1},"collection":"test_collection","operator":"$match","order":0,"options":{}} 
  childTest.equal(JSON.stringify(indexes[0]), JSON.stringify(expectedIndex))

  childTest.end()
})

tap.test('generate indexes for logical operator corner cases', async (childTest) => {
    
  const generator = new Generator()

  const logicfields = generator.processLogicalOperator([{"$and": [ { "$lt": [ "$start", "{$END_DATE}" ] }, { "$gt": [ "$$end", "{$START_DATE}" ] }]}])

  childTest.equal(JSON.stringify({"start":1}), JSON.stringify(logicfields))

  childTest.end()
})

tap.test('generate no index for match sort stages with fields from aliases', async (childTest) => {
  const aggregation = Aggregation.fromJSON(jsonMatchSortAliases)
   
  const generator = new Generator()

  const indexes = generator.generateIndexes(aggregation)

  childTest.equal(indexes.length, 2)


  
  childTest.end()
})

tap.test('generate index for group stage with id field from alias', async (childTest) => {
  const aggregation = Aggregation.fromJSON(jsonAggGroupAliases1)
   
  const generator = new Generator()

  const indexes = generator.generateIndexes(aggregation)

  childTest.equal(indexes.length, 1)
  
  const expectedIndexGroupKey = {"o.age":1,"y":1}
  childTest.equal(JSON.stringify(indexes[0].key), JSON.stringify(expectedIndexGroupKey))

  childTest.end()
})


tap.test('generate no index for group stage with id field from alias', async (childTest) => {
  const aggregation = Aggregation.fromJSON(jsonAggGroupAliases2)
   
  const generator = new Generator()

  const indexes = generator.generateIndexes(aggregation)

  childTest.equal(indexes.length, 2)
  
  const expectedIndexSort = {"name":"test_group_alias2_test_collection_sort_1","key":{"x":1,"o.age":1},"collection":"test_collection","operator":"$sort","order":1,"options":{}}
  childTest.equal(JSON.stringify(indexes[0]), JSON.stringify(expectedIndexSort))

  const expectedIndexGroup = {"name":"test_group_alias2_test_collection_group_2","key":{"x":1,"o.age":1},"collection":"test_collection","operator":"$group","order":2,"options":{}}
  childTest.equal(JSON.stringify(indexes[1]), JSON.stringify(expectedIndexGroup))

  childTest.end()
})

tap.test('generate no index for group stage with id field from alias and previous sort', async (childTest) => {
  const aggregation = Aggregation.fromJSON(jsonAggGroupAliases3)
   
  const generator = new Generator()

  const indexes = generator.generateIndexes(aggregation)

  childTest.equal(indexes.length, 1)
  childTest.equal(indexes[0].operator, "$sort")
  childTest.end()
})

tap.test('generate index for group stage should includes first field from alias', async (childTest) => {
  const aggregation = Aggregation.fromJSON(jsonAggGroupAliases4)
   
  const generator = new Generator()

  const indexes = generator.generateIndexes(aggregation)

  childTest.equal(indexes.length, 1)
  childTest.equal(JSON.stringify(indexes[0].key), JSON.stringify({"x": 1,"o.age":1}))
  childTest.end()
})

tap.test('generate index for match stage with multiple aliases uses original field', async (childTest) => {
  const aggregation = Aggregation.fromJSON(jsonMatchMultipleAliases1)
   
  const generator = new Generator()

  const indexes = generator.generateIndexes(aggregation)

  childTest.equal(indexes.length, 1)
  childTest.equal(JSON.stringify(indexes[0].key), JSON.stringify({"o.age": 1}))
  childTest.end()
})

tap.test('generate index for match stage with multiple aliases from project operator uses original fields', async (childTest) => {
  const aggregation = Aggregation.fromJSON(jsonMatchMultipleAliases2)
   
  const generator = new Generator()

  const indexes = generator.generateIndexes(aggregation)

  childTest.equal(indexes.length, 1)
  childTest.equal(JSON.stringify(indexes[0].key), JSON.stringify({"o.age": 1,"payment":1}))
  childTest.end()
})

tap.test('generate index for match stage with multiple aliases on subfields uses original fields', async (childTest) => {
  const aggregation = Aggregation.fromJSON(jsonMatchPartialAliases)
   
  const generator = new Generator()

  const indexes = generator.generateIndexes(aggregation)

  childTest.equal(indexes.length, 1)
  childTest.equal(JSON.stringify(indexes[0].key), JSON.stringify({"edges.size":1,"edges.count":1}))
  childTest.end()
})

tap.test('generate index for match stage with complex expr', async (childTest) => {
  const aggregation = Aggregation.fromJSON(jsonComplexExpr)
   
  const generator = new Generator()

  const indexes = generator.generateIndexes(aggregation)

  childTest.equal(indexes.length, 1)
  childTest.equal(JSON.stringify(indexes[0].key), JSON.stringify({"username":1}))
  childTest.end()
})

tap.test('generate index for match stage with logical op with complex expr', async (childTest) => {
  const aggregation = Aggregation.fromJSON(jsonLogicalComplexExpr)
   
  const generator = new Generator()

  const indexes = generator.generateIndexes(aggregation)

  childTest.equal(indexes.length, 1)
  childTest.equal(JSON.stringify(indexes[0].key), JSON.stringify({"employee":1,"date":1}))
  childTest.end()
})

// test cleanup
tap.teardown(async function() { 

})
  
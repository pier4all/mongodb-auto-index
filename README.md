# Automatic Indexing for MongoDB

This module generates the usable indexes from a set of MongoDB aggregations. 
It does not require a connection to a database or the existence of data, it suggests indexes based only in the queries.

We performed intensive testing that showed that all the suggested indexes were actually used by MongoDB when executing the queries. The code for the checking of the index usage is available at https://github.com/pier4all/auto-index

### Installation
In order to use the package just install it using npm:
```
npm install mongodb-auto-index
```

### Basic usage
This package does not require the installation of any dependency.

The simplest way of using this module is to store each MongoDB query or aggregation in a file as JSON (using ".json" as extension).

For example, an aggregation pipeline is stored in a file called "aggregation_1.json" with the following contents:

```
{
  "aggregate":"test-aggregation-no-coll-copy",
  "pipeline":[
    {
      "$match":{
        "name": "Carpet"
      }
    },
    {
      "$sort":{
        "price": 1
      }
    }
  ],
  "allowDiskUse":true,
  "maxTimeMS":0,
  "cursor":{}
}
```

It is also possible to specify a query as a JSON file, as in the example below:
```
{
  "name":"test-query-01",
  "query":{
      "color": "red",
      "round": false
  },
  "sort": {
     "shape":1,
     "size":1
  },
  "collection": "figures",
  "allowDiskUse":true,
  "maxTimeMS":0,
  "cursor":{}
}
```

```javascript
// import autoindex
const AutoIndex = require('mongodb-auto-index/source/autoindex')

// set the input directory containing the .json query and aggregation files
let inputDir = "./data"

// instantiate AutoIndex
let autoindex = new AutoIndex()
let result = autoindex.autoindex(inputDir) 

// print results
console.log(result)

// It is also possible to save the index results to a file by specifying an output directory
let outputDir = "./output"


```
If an output directory is specified, the module will create a file with a timestamp included in the name containing all the information of the indexes to make easier creating them in MongoDB.
For example a file "indexes_2023-01-26_20-43-20.json" contains the indexes below:
```
{
  "indexes": [
    {
      "createIndexes": "test_collection",
      "indexes": [
        {
          "key": {
            "name": 1,
            "price": 1
          },
          "name": "combine_sort_match_name_price_idx"
        }
      ]
    },
    {
      "createIndexes": "figures",
      "indexes": [
        {
          "key": {
            "color": 1,
            "round": 1,
            "shape": 1,
            "size": 1
          },
          "name": "combine_sort_match_color_round_shape_size_idx"
        }
      ]
    }
  ]
}
```

### Minimize Option
The automatic indexing has the option "minimize" (default is false) that when set to true,  produces the minimal indexes used by the set of queries. It removes from the list the indexes that are contained in other indexes. This usually results on a smaller set of compund indexes.



exports.expectedResultMinimizeFalse = [
    {
      name: 'combine_match_field_idx',
      key: { field: 1 },
      collection: 'test_collection',
      operator: '$match',
      order: 0,
      options: {}
    },
    {
      name: 'combine_sort_match_name_price_idx',
      key: { name: 1, price: 1 },
      collection: 'default',
      operator: '$match$sort',
      order: 0,
      options: {}
    },
    {
      name: 'combine_sort_price_idx',
      key: { price: 1 },
      collection: 'default',
      operator: '$sort',
      order: 0,
      options: {}
    },
    {
      name: 'combine_match_name_idx',
      key: { name: 1 },
      collection: 'default',
      operator: '$match',
      order: 0,
      options: {}
    },
    {
      name: 'combine_sort_match_color_round_shape_size_idx',
      key: { color: 1, round: 1, shape: 1, size: 1 },
      collection: 'figures',
      operator: '$match$sort',
      order: 0,
      options: {}
    },
    {
        name: 'combine_sort_shape_size_idx',
      key: { shape: 1, size: 1 },
      collection: 'figures',
      operator: '$sort',
      order: 0,
      options: {}
    },
    {
        name: 'combine_match_color_round_idx',
      key: { color: 1, round: 1 },
      collection: 'figures',
      operator: '$match',
      order: 0,
      options: {}
    }
]

exports.expectedResultMinimizeTrue = [
  {
    name: 'combine_match_field_idx',
    key: { field: 1 },
    collection: 'test_collection',
    operator: '$match',
    order: 0,
    options: {}
  },
  {
    name: 'combine_sort_match_name_price_idx',
    key: { name: 1, price: 1 },
    collection: 'default',
    operator: '$match$sort',
    order: 0,
    options: {}
  },
  {
    name: 'combine_sort_match_color_round_shape_size_idx',
    key: { color: 1, round: 1, shape: 1, size: 1 },
    collection: 'figures',
    operator: '$match$sort',
    order: 0,
    options: {}
  }
]

exports.expectedResultExportMinimizeTrue = {
  "indexes": [
    {
      "createIndexes": "test_collection",
      "indexes": [
        {
          "key": {
            "field": 1
          },
          "name": "combine_match_field_idx"
        }
      ]
    },
    {
      "createIndexes": "default",
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
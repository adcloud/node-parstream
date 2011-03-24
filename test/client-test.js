var vows = require("vows")
  , assert = require("assert")
  , parstream = require("../lib/parstream.js")

vows.describe("client")
  .addBatch({
    'if parstream is not running': {
      'and a client tries to connect': {
        topic: function() {
          var client = parstream.createClient()
            , cb = this.callback
          client.connect(function(err) {
            cb(undefined, err)
          })
        },
        'an exception should be raised': function(err) {
          assert.equal(err.code, 'ECONNREFUSED')
        }
      }
    }
  })
  .addBatch({
    'if parstream is running': {

      // create parstream mock
      topic: function() {
        mock = require("./mock").create(this.callback)
      },

      // connecting
      'a client tries to connect': {
        topic: function(server, mockData) {
          var client = parstream.createClient()
            , cb = this.callback

          client.connect(function(err) {
            cb(undefined, err, server, client, mockData)
          })
        },
        'no exception should be raised': function(stupid, err) {
          assert.isUndefined(err)
        },

        // querying
        'and sends a query': query(
          "select wage_id from Wages where wage_id=47", {
            'there should be a resultset': hasResultset(),

            // reusing the socket
            'and another query afterwards': query(
              "select wage_id from Wages where wage_id=47", {
                'there should be another resultset': hasResultset(),

                // destroying the mock
                'parstream mock is destroyed': {
                  topic: function(data, server, client, mockData) {
                    server.close()

                    this.callback()
                  },
                  'done': function() {}
                }
              }
            )
          }
        ),

        // queueing
        'and sends a query on parallel': query(
          "select wage_id from Wages where wage_id=47", {
            'there should be a resultset': hasResultset()
          }
        ),

        // error handling
        'and sends a query with an error': query(
          "select y_wage_id from Wages where wage_id=47", {
            'there should be an error': hasError()
          }
        ),

        // description tree
        'and sends a description tree': query(parstream.node('output', {
            fieldList: ["wage_id"],
            children: [
              parstream.node('fetch', {
                fieldList: ["wage_id"],
                filter: "wage_id=47",
                tableName: "Wages"
              })
            ]
          }), {
            'there should be a resultset': hasResultset()
          }
        )
      }
    }
  })
.export(module)

function query(query, vows) {
  var context = {
    topic: function(err, server, client, mockData) {
      var cb = this.callback

      client.query(query, function(err, data) {
        cb(err, data, server, client, mockData)
      })
    }
  }
  Object.keys(vows).forEach(function(vow) {
    context[vow] = vows[vow]
  })

  return context
}

function hasResultset() {
  return function(data) {
    assert.isTrue(typeof data === 'object')
    assert.isTrue(typeof data.rows === 'object')
  }
}

function hasError() {
  return function(data) {
    assert.isTrue(typeof data === 'object')
    assert.isTrue(typeof data.error === 'string')
  }
}


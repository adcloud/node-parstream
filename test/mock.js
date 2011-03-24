var net = require('net')
  , exports = module.exports = Mock = {}
  , delimiter = "\n\n"

Mock.create = function(port, cb) {
  var receivedCommands = []
    , data = ''

  if (typeof port === 'function') {
    cb = port
    port = 9042
  }

  var received

  var server = net.createServer(function(c) {
    var write = function(msg) {
      if (c.writable) c.write(msg + delimiter)
    }
    c.on('data', function(chunk) {
      chunk = chunk.toString()
      if (chunk.match(/\r\n/)) {
        var cmd = chunk.replace(/\r\n/, '')
        receivedCommands.push(cmd)

        if (cmd === '{"sql_command":"select wage_id from Wages where wage_id=47"}') {
          write('{"rows":[{"row":{"wage_id":47}}],"rowcount":1}')
        } else if (cmd === '{"parstream::OutputNode":{"fieldList":["wage_id"],"children":[{"parstream::FetchNode":{"fieldList":["wage_id"],"filter":"wage_id=47","tableName":"Wages","children":[],"alias":""}}],"format":"default","limit":0,"offset":0}}') {
          write('{"rows":[{"row":{"wage_id":47}}],"rowcount":1}')
        } else {
          write('{"error" : "some error" }')
        }
      }
    })

    setTimeout(function() {
      c.end()
    }, Math.random() * 10)
  })
  server.listen(port, '127.0.0.1', function() {
    cb(undefined, server, receivedCommands)
  })
}


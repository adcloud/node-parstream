/*!
 * Parstream client for Node
 *
 * Requires Parstream >= 1.6.0
 *
 * Copyright(c) 2011 Adcloud GmbH
 * MIT Licensed
 */
var net = require('net')

var exports = module.exports = Parstream = {}

/** Connection Pool **/
Parstream.createPool = function createPool(options) {
  var self = new Pool()

  options = options || {}
  self.size = options.size || 5
  self.clients = []

  for (var i = 0; i < self.size; i++) {
    self.clients[i] = Parstream.createClient(options)
  }

  // create pool methods dynamically (wrap clients)
  Object.keys(Client.prototype).forEach(function(item) {
    if (typeof self[item] === 'undefined' && typeof Client.prototype[item] === 'function') {
      // copy client methods to pool
      self[item] = function() {
        var args = [].slice.call(arguments)
          , selectedClient = this.selectClient()

        this.clients[selectedClient][item].apply(
          this.clients[selectedClient],
          args
        )
      }
    }
  })

  return self
}
function Pool() {}
Pool.prototype.selectClient = function poolSelectClient() {
  return Math.floor(Math.random() * this.size)
}
Pool.prototype.connect = function poolConnect(cb) {
  var connected = 0
    , errors = 0
    , self = this
    , clients = self.clients.length

  self.clients.forEach(function(client) {
    client.connect(function(err) {
      if (err) errors++

      if (++connected === clients) {
        if (errors !== clients) err = undefined
        cb(err)
      }
    })
  })
}
Pool.prototype.close = function poolClose() {
  var self = this

  self.clients.forEach(function(client) {
    client.close()
  })
}

/** Parstream client **/
Parstream.createClient = function createClient(options) {
  var self = new Client()

  options = options || {}
  self.host = options.host || '127.0.0.1'
  self.port = options.port || 9042

  self.connected = false
  self.reconnect = true
  self.requestQueue = []
  self.current = null

  return self
}
function Client() {}
Client.prototype.connect = function connect(cb) {
  var self = this
  self.socket = net.createConnection(self.port, self.host)
  self.socket.on('connect', function() {
    self.socket.setEncoding('utf8')

    var data = ''

    // receive data
    self.socket.on('data', function(chunk) {

      if (self.current !== null) {
        chunk = chunk.toString()
        data += chunk
        if (chunk.match(/\n\n/)) {
          data = data.replace(/\n\n/, '')

          self.current.callback(undefined, data)
          data = ''

          if (self.requestQueue.length > 0) {
            var args = self.requestQueue.shift()

            self.current.payload = args.payload
            self.current.callback = args.callback
            self.write(args.payload)
          } else self.current = null
        }
      }
    })
    self.socket.on('end', function() {
      console.log('socket ended')

      // reconnect client if it died
      if (self.reconnect) self.connect(function(err) {
        if (err) console.log('client failed')
        else {
          if (self.current !== null) {
            self.write(self.current.payload)
          }
        }
      })
    })

    self.connected = true
    cb()
  })
  self.socket.on('error', function(err) {
    self.socket.destroy()
    cb(err)
  })
}
Client.prototype.query = function(query, cb) {
  if (!this.connected) cb(new Error('socket not connected'))

  this.queue(JSON.stringify({sql_command: query}), function(err, data) {
    if (err) return cb(err)

    try {
      data = JSON.parse(data)
    } catch(err) {
      return cb(err)
    }
    cb(undefined, data)
  })
}
Client.prototype.help = function(cmd) {
  if (!this.connected) cb(new Error('socket not connected'))

  if (cmd) cmd = 'help ' + cmd
  else cmd = 'help'

  this.queue(cmd, function(err, data) {
    if (err) throw err
    console.log(data)
  })
}
Client.createCmd = function(cmd) {
  return function() {
    if (!this.connected) cb(new Error('socket not connected'))

    this.queue(cmd, function(err, data) {
      if (err) throw err
      console.log(data)
    })
  }
}
Client.prototype.partitions = Client.createCmd('info partitions')
Client.prototype.memory = Client.createCmd('info memory')
Client.prototype.queue = function request(payload, cb) {
  var self = this

  if (self.current !== null) {
    // add request to queue
    return self.requestQueue.push({
      payload:payload,
      callback:cb
    })
  }

  // queue is empty – start right away
  self.current = {
    payload: payload,
    callback: cb
  }
  self.write(payload)
}
Client.prototype.write = function(payload) {
  this.socket.write(payload + "\r\n")
}
Client.prototype.close = function clientClose() {
  this.reconnect = false
  this.socket.end()
}


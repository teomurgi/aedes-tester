'use strict'

var urlModule = require('url')
var mongodb = require('mongodb')
var MongoClient = mongodb.MongoClient
var inherits = require('inherits')
var MQEmitter = require('mqemitter')
var through = require('through2')
var pump = require('pump')
var nextTick = process.nextTick
var EE = require('events').EventEmitter

function MQEmitterMongoDB (opts) {
  if (!(this instanceof MQEmitterMongoDB)) {
    return new MQEmitterMongoDB(opts)
  }

  opts = opts || {}
  opts.size = opts.size || 10 * 1024 * 1024 // 10 MB
  opts.max = opts.max || 10000 // documents
  opts.collection = opts.collection || 'pubsub'

  var url = opts.url || 'mongodb://127.0.0.1/mqemitter'

  this._opts = opts

  var that = this

  this._db = null

  if (opts.db) {
    that._db = opts.db
    waitStartup()
  } else {
    MongoClient.connect(url, function (err, client) {
      if (err) {
        return that.status.emit('error', err)
      }

      var urlParsed = urlModule.parse(that._opts.url)
      var databaseName = urlParsed.pathname ? urlParsed.pathname.substr(1) : undefined

      that._client = client
      that._db = client.db(databaseName)
      waitStartup()
    })
  }

  this._started = false
  this.status = new EE()

  function waitStartup () {
    that._collection = that._db.collection(opts.collection)
    that._collection.isCapped(function (err, capped) {
      if (that.closed) { return }

      if (err) {
        // if it errs here, the collection might not exist
        that._db.createCollection(opts.collection, {
          capped: true,
          size: opts.size,
          max: opts.max
        }, start)
      } else if (!capped) {
        // the collection is not capped, make it so
        that._db.command({
          convertToCapped: opts.collection,
          size: opts.size,
          max: opts.max
        }, start)
      } else {
        start()
      }
    })
  }

  var oldEmit = MQEmitter.prototype.emit

  this._waiting = {}

  setInterval(() => {
    console.log("_waiting ->", Object.keys(that._waiting).length)
  }, 3500);

  this._lastId = new mongodb.ObjectId()

  var failures = 0

  function start () {
    that._stream = that._collection.find({
      _id: { $gt: that._lastId }
    }, {
      tailable: true,
      timeout: false,
      awaitData: true,
      numberOfRetries: -1
    })

    pump(that._stream, through.obj(process), function () {
      if (that.closed) {
        return
      }

      if (that._started && ++failures === 10) {
        that.status.emit('error', new Error('Lost connection to MongoDB'))
      }
      setTimeout(start, 100)
    })

    that.status.emit('stream')

    function process (obj, enc, cb) {
      if (that.closed) {
        return cb()
      }

      // convert mongo binary to buffer
      if (obj.payload && obj.payload._bsontype) {
        obj.payload = obj.payload.read(0, obj.payload.length())
      }

      that._started = true
      failures = 0
      that._lastId = obj._id
      oldEmit.call(that, obj, cb)

      var id = obj._id.toString()
      if (that._waiting[id]) {
        nextTick(that._waiting[id])
        delete that._waiting[id]
      }
    }
  }
  MQEmitter.call(this, opts)
}

inherits(MQEmitterMongoDB, MQEmitter)

MQEmitterMongoDB.prototype.emit = function (obj, cb) {
  var that = this
  var err

  if (!this.closed && !this._stream) {
    // actively poll if stream is available
    this.status.once('stream', this.emit.bind(this, obj, cb))
    return this
  } else if (this.closed) {
    err = new Error('MQEmitterMongoDB is closed')
    if (cb) {
      cb(err)
    } else {
      throw err
    }
  } else {
    this._collection.insert(obj, function (err, res) {
      if (cb) {
        if (err) {
          cb(err)
          return
        }

        var obj = res.ops[0]
        var id = obj._id.toString()
        var lastId = that._lastId.toString()
        if (id > lastId) {
          that._waiting[id] = cb
        } else {
          cb()
        }
      }
    })
  }
  return this
}

MQEmitterMongoDB.prototype.close = function (cb) {
  cb = cb || noop

  if (this.closed) {
    return cb()
  }

  if (!this._stream) {
    this.status.once('stream', this.close.bind(this, cb))
    return
  }

  this._stream.destroy()
  this._stream.on('error', function () {})
  this._stream = null

  this.closed = true

  var that = this
  MQEmitter.prototype.close.call(this, function () {
    if (that._opts.db) {
      cb()
    } else {
      that._client.close(cb)
    }
  })

  return this
}

function noop () {}

module.exports = MQEmitterMongoDB

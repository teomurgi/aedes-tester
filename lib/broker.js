const net = require('net');
const Aedes = require('aedes');
const AedesMongodbPersistence = require('aedes-persistence-mongodb');
const MQEmitterMongodb = require('mqemitter-mongodb');

class Broker {
  constructor({ mqttPort, mongoConnectionString }) {
    this.mqttPort = mqttPort;
    this.mongoConnectionString = mongoConnectionString;

    this._tmpCount = 0;
  }

  async start() {
    return new Promise((resolve, reject) => {
      const broker = new Aedes(this._brokerOptions());
      const server = net.createServer(broker.handle);
      return server.listen(this.mqttPort, (err) => {
        if (err) {
          console.log(`Listen server error: ${err.message}`);
          return reject(err);
        }
        console.log(`Listening on port ${this.mqttPort}`);
        return resolve(broker);
      });
    });
  }

  _brokerOptions() {
    const self = this;
    return {
      mq: this._mqemitter(),
      persistence: this._persistence(),
      authenticate: (client, username, password, callback) => callback(null, true),
      published: (packet, _, callback) => {
        if (!packet.topic.startsWith('$SYS/')) {
          self._tmpCount += 1;
          console.log(`published -> ${self._tmpCount} ${packet.topic}`);
          callback(null);
        } else {
          callback(null);
        }
      },
    };
  }

  _mqemitter() {
    const mq = new MQEmitterMongodb({ url: this.mongoConnectionString });
    mq.on('error', (err) => {
      console.log(`MQEmitterMongodb error: ${err.message}`);
    });
    return mq;
  }

  _persistence() {
    const persistence = new AedesMongodbPersistence({ url: this.mongoConnectionString });
    persistence.on('error', (err) => {
      console.log(`AedesMongodbPersistence error: ${err}`);
    });
    return persistence;
  }
}

module.exports = Broker;

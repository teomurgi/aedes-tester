const mqtt = require('mqtt');

class Asset {
  constructor({ url, qos, retain }) {
    this.mqttQos = qos;
    this.mqttRetain = retain;
    this.url = url;
    this.assetName = 'ASSET_01';
    this.feedAddress = 'FEED_01';
    this.client = null;
    this.stats = {};
  }

  async start() {
    await this._clientConnect();
    setInterval(() => {
      this.send();
    }, 4000);
    this.send();
  }

  async _clientConnect() {
    this.client = mqtt.connect(this.url, {
      clientId: this.assetName,
      queueQoSZero: false,
    });
    this.client.on('connect', () => {
      this._incStatistic('clientConnections');
      console.log(`MQTT ${this.assetName} client connected`);
    });
    this.client.on('offline', () => {
      this._incStatistic('clientDisconnections');
      console.log(`MQTT ${this.assetName} client offline`);
    });
    this.client.on('error', (err) => {
      this._incStatistic('clientErrors');
      console.log(`MQTT ${this.assetname} client error: ${err.message}`);
    });
    return this._waitConnect();
  }

  async _waitConnect() {
    return new Promise((resolve) => {
      const interval = setInterval(() => {
        if (this.client.connected) {
          resolve();
        }
      }, 10);
      setTimeout(() => {
        clearInterval(interval);
        resolve();
      }, 5000);
    });
  }

  async send() {
    const promises = [];
    for (let i = 0; i < 1000; i++) {
      const topic = this._covTopic(this.assetName, this.feedAddress);
      const payload = this._covPayload(i + 1);
      promises.push(this._publish(topic, payload));
    }
    await Promise.all(promises);
    this._logStats();
  }

  _covTopic(assetName, feedAddress) {
    return `de/${assetName}/${feedAddress}`;
  }

  _covPayload(value) {
    const now = new Date();
    return {
      address: this.feedAddress,
      qos: 'good',
      payload: value,
      timestampDevice: now.getTime(),
    };
  }

  async _publish(topic, payload) {
    return new Promise((resolve) => {
      console.log('MQTT publish:', topic, payload.payload);
      this.client.publish(
        topic, JSON.stringify(payload),
        { qos: this.mqttQos, retain: this.mqttRetain }, (err) => {
          if (err) {
            console.log(`MQTT publish error: ${err.message}`);
            this._incStatistic('publishErrors');
          } else {
            this._incStatistic('publish');
          }
          resolve();
        },
      );
    });
  }

  _incStatistic(name) {
    this.stats[name] = this.stats[name] || 0;
    this.stats[name] += 1;
  }

  _logStats() {
    console.log('statistics', this.stats);
  }
}

module.exports = Asset;

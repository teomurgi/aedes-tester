const Broker = require('./lib/broker');

const broker = new Broker({
  mqttPort: 1883,
  mongoConnectionString: 'mongodb://localhost:27017/mactest?poolSize=2',
});
broker.start();

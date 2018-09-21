const Asset = require('./lib/asset');

const asset = new Asset({
  url: 'mqtt://broker:1883',
  qos: 0,
  retain: false,
});
asset.start();

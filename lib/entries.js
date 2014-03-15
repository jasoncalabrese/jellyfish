var mongojs = require('mongojs');
var config = require('../env.js');
var crypto = require('crypto');
var base32hex = require('amoeba').base32hex;
var moment = require('moment');
var log = require('./log.js')('entries.js');

module.exports = (function() {
  var db = mongojs(config.mongoConnectionString, ['deviceData']);
  return {
    add: function(entry, cb) {
      entry.deviceTime = entry.deviceTime || moment().format('YYYY-MM-DDTHH:mm:ss');
      var hasher = crypto.createHash('sha1');
      if (entry._id == null) {
        hasher.update(entry.type);
        hasher.update(String(entry.value));
        hasher.update(entry.deviceTime);
        hasher.update(entry.groupId);
        entry._id = base32hex.encodeBuffer(hasher.digest(), { paddingChar: '-' });
      } else {
        hasher.update(entry._id);
        hasher.update(entry.groupId);
        entry._id = base32hex.encodeBuffer(hasher.digest(), { paddingChar: '-' });
      }
      db.deviceData.save(entry, cb);
    }
  }
})();
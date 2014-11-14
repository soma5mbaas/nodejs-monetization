var MonetizationBuffer = require('./MonetizationBuffer');
var _ = require('underscore');
var util = require('util');
var store = require('haru-nodejs-store');
var moment = require('moment');


var _buffer = new MonetizationBuffer(
   require('../config').MonetizationBuffer
);

_buffer.on('full', on_full);
_buffer.on('timeout', on_timeout);

exports.save = function(msg, callback) {
    var _moment = moment(msg.timestamp);

    var monetization = {
        applicationid: msg.applicationId,
        userid: msg.monetization.userId,
        productname: msg.monetization.productName,
        currencycode: msg.monetization.currencyCode,
        price: msg.monetization.price,
        national: msg.monetization.national,
        appversion: msg.monetization.appVersion,
        androidversion: msg.monetization.androidVersion,
        device: msg.monetization.device,
        buydate: _moment.format('YYYY-MM-DD'),
        buytime:  _moment.format('hh:00')
    };

    _buffer.put(monetization);

    if( callback ) { return callback(null, null); }
};

function on_full(buffer) {
    var cloneBuffer = _.clone(buffer._buffer);

    _insertQuery(cloneBuffer, function(error, results) {
    });
};

function on_timeout(buffer) {
    var cloneBuffer = _.clone(buffer._buffer);

    _insertQuery(cloneBuffer, function(error, results) {
    });
}


function _insertQuery(datas, callback) {
    if(datas.length < 1) { return callback(null, null); }

    var bulk = [];
    for( var i = 0; i < datas.length; i++ ) {
        var obj = datas[i];
        bulk.push(_.values(obj));
    }

    var query = util.format('insert into Monetization (%s) VALUES ?', _.keys(obj));

    store.get('mysql').query(query,  [bulk], callback);

};
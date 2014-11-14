var amqp = require('amqplib/callback_api');
var async = require('async');


function RabbitMQ(config) {
    this.queues = config;
    this.connection = {};

    this.connect();
};

RabbitMQ.prototype.connect = function() {
    var self = this;

    Object.keys(self.queues).forEach(function(queueName) {
        var queue =  self.queues[queueName];

        amqp.connect( queue.url, function(error, conn) {
            self.connection[queueName] = conn;
            log.info('[%d] rabbitmq %s connected', process.pid, queueName);
        });
    })

};

RabbitMQ.prototype.publish = function(qname, data, callback) {
    var self = this;

    async.waterfall([
            function checkConnect(callback) {
                var conn = self.connection[qname];
                var error = null;

                if(conn === null || conn === undefined) {
                    error = new Error('RabbitMQ Connect error');
                }

                callback( error, conn );
            },
            function createChannel(conn, callback) {
                conn.createChannel(function(error, channel) {
                    callback( error, channel );
                });
            },
            function assertQueue(channel, callback) {
                channel.assertQueue(qname, {durable: true, exclusive: false}, function(error, ok) {
                    callback( error, ok, channel );
                });
            },
            function sendToQueue(ok, channel, callback) {
                var strData = JSON.stringify(data);
                var error = null;

                // log.info('[%d] send to %s data : %s', process.pid, qname, strData);
                if( !data.method )
                    log.debug('[%d] method : %s', process.pid, strData );

                channel.sendToQueue(qname, new Buffer(strData));
                channel.close();

                callback( error, null );
            }
        ],
        function done(error, result) {
            if( error ) { log.error('[%d] Queue(%s) error : %s', process.pid, qname, error.message); }

            if( typeof callback === 'function' ) {
                return callback( error, result );
            }
        });
};

module.exports = RabbitMQ;
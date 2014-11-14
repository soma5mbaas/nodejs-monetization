/**
 * Created by syntaxfish on 14. 11. 6..
 */
var amqp = require('amqplib/callback_api');
var queue = require('./config').mqueue.monetization;

var monetization = require('./lib/monetization');

var config = require('./config');
var store = require('haru-nodejs-store');

var RabbitMq = require('./connectors/rabbitmq');
var rabbitmq = new RabbitMq({monetization: config.mqueue.monetization});

var _ = require('underscore');


store.connect(config.store);

function bail(err, conn) {
    console.error(err);
    if (conn) conn.close(function() { process.exit(1); });
}

function on_connect(err, conn) {
    if (err !== null) return bail(err);
    process.once('SIGINT', function() { conn.close(); });

    var q = 'monetization';

    conn.createChannel(function(err, ch) {
        if (err !== null) return bail(err, conn);
        ch.assertQueue(q, {durable: true}, function(err, _ok) {
            ch.consume(q, doWork, {noAck: false});
            console.log(" [*] Waiting for messages. To exit press CTRL+C");
        });

        function doWork(msg) {
            var body = JSON.parse(msg.content);
            monetization.save(body, function(error, results) {
                ch.ack(msg);
            });
        }
    });

    conn.on('close', function(error) {
        //console.log('[close] : ', error);
    });

    conn.on('error', function(error) {
        console.log('[error] : ',error);
        setTimeout(function(){
            console.log('try reconnect...');
            amqp.connect(queue.url, on_connect);
        }, 1000);
    });


}

amqp.connect(queue.url, on_connect);
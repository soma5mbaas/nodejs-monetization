
var inherits = require('util').inherits;
var EventEmitter = require('events').EventEmitter;
var _ = require('underscore');

module.exports = MonetizationBuffer;

function MonetizationBuffer(options) {
    this._options = options;
    this._buffer = [];
}

inherits(MonetizationBuffer, EventEmitter);


MonetizationBuffer.prototype.put = function(obj) {
    var self = this;
    if( self._buffer.length < self._options.bufferSize ) {
        self._buffer.push(obj);
    } else {
        self.emit('full', self);

        if( self._options.autoFlush ) {
            self._buffer.splice(0, self._options.bufferSize);
            self._buffer.push(obj);
        }
    }

    if( self._options.timeout && self._buffer.length === 1 ) {
        setTimeout(function(){
            self.emit('timeout', self);
            self._buffer.splice(0, self._options.bufferSize);
        }, self._options.timeout);
    }
};

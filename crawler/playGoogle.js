/**
 * Created by syntaxfish on 14. 11. 1..
 */
var request = require('request');
var config = require('./playGoogle.json');
var _ = require('underscore');
var parser = require('whacko');
var async = require('async');
var moment = require('moment');
var util = require('util');

var keys = require('haru-nodejs-util').keys;
var store = require('haru-nodejs-store');

exports.crawling = function(option, callback) {
    async.waterfall([
        function crawling(callback) {
            _crawling(option, function(error, results) {
                callback(error, results);
            });
        },
        function insertQuery(results, callback) {
            _insertQuery(option, results, function(error, results) {
                callback(error, results);
            });
        }
    ], function done(error, results) {
        if(callback){ return callback(error, results); }
    });
};

exports.requestSuccessUrl = function(body) {
    request.get({
        url: util.format(config.successUrl),
        timeout: 500
    }, body.applicationId);
};



function _insertQuery(options, datas, callback) {
    var marketCommentIdKey = keys.marketCommentIdKey(options.applicationId, options.market, options.location);

    var multi = store.get('public').multi();
    for( var i = 0; i < datas.length; i++ ) {
        multi.sadd(marketCommentIdKey, datas[i].commentid);
    }
    multi.exec(function(error, results) {
        var count = 0;
        var bulk = [];

        for(var i = 0; results && i < results.length; i++ ) {
            if( results[i] ) { bulk.push(_.values(datas[i])); }
            count += results[i];
        }

        console.log('[%s] p: %d, count: %d', options.market, options.page, count);

        if( count === 0 ) { return callback(new Error('ER_DUP_ENTRY'), []); }

        store.get('mysql').query('insert into Reviews (commentid, imagesource, name, date, rating, title, body, applicationid, location, market, strdate) VALUES ?', [bulk], callback);
    });
}


function _crawling(body, callback) {
    _request(config.url, {id: body.packageName,
        reviewSortOrder: 0,
        reviewType: 0,
        pageNum: body.page,
        xhr: 1,
        hl: body.location}, function(error, res, html) {

        html = html.replace('\" ', '').replace(' \"', '').replace(/\\u003c/g, '<').replace(/\\u003e/g, '>').replace(/\\u003d\\/g, '=').replace(/\\\"/g, '"');

        var mainSelector = config.mainSelector;
        var reviews = [];

        var $ = parser.load(html);

        var el = mainSelector;// + ' ' + selector.selector;
        var match = $(el);

        var format;
        var locale;
        var parseRating = _makeParseRating(body.location);
        if( body.location === 'ko' ) {
            // 2014년 11월 3일
            format = 'YYYY년 MM월 DD일';
        } else if( body.location == 'en') {
            // October 16, 2014
            format = 'MMM DD, YYYY';
            locale = 'en';
        } else if( body.location == 'ja') {
            // 2014年7月9日
            format = 'YYYY年MM月DD日';
            locale = 'ja';
        } else if( body.location == 'de') {
            // 11. Februar 2013
            format = 'DD. MMM YYYY';
            locale = 'de';
        } else if( body.location == 'fr') {
            // 17 novembre 2012
            format = 'DD MMM YYYY';
            locale = 'fr';
        }

        for( var i = 0; i < match.length; i++ ){
            var id = $(mainSelector).find('div.review-header')[i].attribs['data-reviewid'];
            var auth_image = $(mainSelector).find('a img.author-image')[i] ? $(mainSelector).find('a img.author-image')[i].attribs.src : '';
            var name = $(mainSelector).find('.review-header .review-info .author-name a')[i] ? $(mainSelector).find('.review-header .review-info .author-name a')[i].children[0].data : '';
            var date = $(mainSelector).find('.review-header .review-info .review-date')[i] ? $(mainSelector).find('.review-header .review-info .review-date')[i].children[0].data : '';
            var rating_string = $(mainSelector).find('.review-header .review-info .review-info-star-rating .tiny-star.star-rating-non-editable-container')[i] ? $(mainSelector).find('.review-header .review-info .review-info-star-rating .tiny-star.star-rating-non-editable-container')[i].attribs['aria-label'] : '';
            var title = $(mainSelector).find('.review-body span.review-title')[i].children[0] || {data: '', parent:{next: {data: ''}}};

            var rating_number = parseRating(rating_string);

            var _moment =  moment(date, format, locale);
            var utc = _moment.valueOf();
            var strdate =  _moment.format('YYYY-MM-DD');

            reviews.push( {
                commentid: id,
                imagesource: auth_image,
                name: name,
                date: utc,
                rating: rating_number,
                title: title.data,
                body: title.parent.next.data,
                applicationid: body.applicationId,
                location: body.location,
                market: 'playGoogle',
                strdate: strdate
            });
        }

        if(callback){ return callback(error, reviews); }

    });
};

function _makeParseRating(locale) {
    if( locale === 'ko' ) {
        return function(rating_string) {
            return Number(rating_string.split('만점에 ')[1].match(/\d+/)[0]);
        };
    } else if( locale === 'en') {
        return function(rating_string) {
            return Number(rating_string.match(/\d+/)[0]);
        }
    } else if( locale === 'ja') {
        return function(rating_string) {
            return Number(rating_string.split('つ星のうち')[1].match(/\d+/)[0]);
        }
    } else if( locale === 'de') {
        return function(rating_string) {
            return Number(rating_string.match(/\d+/)[0]);
        }
    } else if( locale === 'fr') {
        return function(rating_string) {
            return Number(rating_string.match(/\d+/)[0]);
        }
    }
};

function _request(url, body, callback){
    var  google_store_options = {
        url: url,
        method: 'POST',
        headers: {
            'User-Agent': 'request',
            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
            'Accept-Language': 'ko-KR,ko;q=0.8,en-UxS;q=0.6,en;q=0.4'
        },
        formData: body
    };

    request(google_store_options, callback);
};

/**
 * Created by syntaxfish on 14. 11. 1..
 */
var request = require('request');
var config = require('./amazon.json');
var _ = require('underscore');
var parser = require('whacko');
var util = require('util');
var async = require('async');
var moment = require('moment');

var jconv = require('jconv');

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
    var reviews = [];
    var uri = config.host[body.location] + config.url;
    _request(util.format(uri, body.packageName, body.page, body.page), function (error, res, html) {
        var mainSelector1 = config.mainSelector1;
        var mainSelector2 = config.mainSelector2;

        var $ = parser.load(html);

        var match1 = $(mainSelector1);
        var match2 = $(mainSelector2);

        if( match1.length > 0 ) {
            if(callback){ return callback(error, _getReviewType1($, mainSelector1, body)); }
        } else if(match2.length > 0 && body.location === 'en') {
            if(callback){ return callback(error, _getReviewType2($, mainSelector2, body)); }
        } else {
            if(callback){ return callback(new Error('no match'), null); }
        }
    });
}

function _getReviewType1($, mainSelector, body) {
    var reviews = [];

    $('div.tiny').remove();

    var format;
    var locale;

    if( body.location == 'en') {
        // October 16, 2014
        format = 'MMM DD, YYYY';
        locale = 'en';
    } else if( body.location == 'ja') {
        // 2014/11/6
        format = 'YYYY/MM/DD';
        locale = 'en';
    } else if( body.location == 'de') {
        // 18. Oktober 2014
        format = 'DD. MMM YYYY';
        locale = 'de';
    } else if( body.location == 'fr') {
        // 15 septembre 2014
        format = 'DD MMM YYYY';
        locale = 'fr';
    } else if( body.location == 'cn') {
        // 2014年8月14日
        format ='YYYY年MM月DD日';
    } else if( body.location == 'uk' ) {
        // 3 Nov 2013
        format = 'DD MMMM YYYY';
        locale = 'en';
    } else if( body.location == 'ca') {
        // Jan. 11 2014
        format = 'MMMM. DD YYYY';
        locale = 'en';
    }


    var tableSelector = mainSelector+' tbody tr td:first-of-type';
    var commentids = $(tableSelector + ' > a');
    for( var i = 0; i < commentids.length; i++ ) {
        var commentid = commentids[i].attribs.name; // commentid
        var rating_string = $(tableSelector+' div:nth-of-type('+(i+1)+') > div:nth-last-child(5) > span:nth-child(1) > span')[0].attribs.class;
        var title = $(tableSelector+' div:nth-of-type('+(i+1)+') > div:nth-last-child(5) > span:nth-child(2) > b').text();
        var date = $(tableSelector+' div:nth-of-type('+(i+1)+') > div:nth-last-child(5) > span:nth-child(2) > nobr').text();
        var name =  $(tableSelector+' div:nth-of-type('+(i+1)+') > div:nth-last-child(4) > div:nth-of-type(1) > div:nth-of-type(2) > a:nth-of-type(1) span[style^="font-weight: bold;"] ').text();
        var text = $(tableSelector+' div:nth-of-type('+(i+1)+') div.reviewText').text();

        var utc =  moment(date, format, locale).valueOf();
        var strdate =  moment(date, format, locale).format('YYYY-MM-DD');
        var rating_number = Number(rating_string.split('_')[2].match(/\d+/)[0]);

        reviews.push( {
            commentid: commentid,
            imagesource: '',
            name: name,
            date: utc,
            rating: rating_number,
            title: title,
            body: text,
            applicationid: body.applicationId,
            location: body.location,
            market: 'amazon',
            strdate: strdate
        });
    }

    return reviews;
};

function _getReviewType2($, mainSelector, body) {
    var reviews = [];
    var match = $(mainSelector);

    for( var i = 0; i < match.length; i++ ){
        var id = match[i].attribs.id;
        var name = match.find('div.a-row span.review-byline a.a-link-normal.author')[i].children[0].data;
        var date = match.find('div.a-row span.a-color-secondary.review-date')[i].children[0].data;
        var rating_string =  match.find('div.a-row a.a-link-normal i')[i].attribs.class.split('a-icon-star')[1];
        var rating_number = Number(rating_string.match(/\d+/)[0]);
        var title = match.find('div.a-row a.a-link-normal.review-title.a-color-base.a-text-normal.a-text-bold')[i].children[0].data;
        var _body = match.find('div.a-row.a-spacing-top-mini.review-data div.a-section.review-text')[i].children[0].data;

        var utc = moment(date, 'YYYY년 MM월 DD일').valueOf();
        var strdate =  moment(date, 'YYYY년 MM월 DD일').format('YYYY-MM-DD');

        reviews.push( {
            commentid: id,
            imagesource: '',
            name: name,
            date: utc,
            rating: rating_number,
            title: title,
            body: _body,
            applicationid: body.applicationId,
            location: body.location,
            market: 'amazon',
            strdate: strdate
        });

    }

    return reviews;
};

function _request(url, callback){
    var  amazon_options = {
        url: url,
        method: 'GET',
        headers: {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.111 Safari/537.36',
            'Accept-Language': 'ko-KR,ko;q=0.8,en-UxS;q=0.6,en;q=0.4',
            'Accept-Encoding': 'utf-8',
            'Accept-Charset': 'utf-8',
            'Content-Type': 'text/html; charset=utf-8',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Host':'www.amazon.co.jp',

            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
    };

    request(amazon_options, callback);
};

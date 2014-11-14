var express = require('express');
var router = express.Router();

var review = require('../controllers/review');

router.post('/fetch', review.fetch);

module.exports = router;

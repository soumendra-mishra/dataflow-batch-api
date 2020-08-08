'use strict';

require('dotenv').config();
const bodyParser = require('body-parser');
const cors = require('cors');
const express = require('express');
const app = express();

const dataflowSubmitBatchController = require('./controllers/dataflowSubmitBatchController');

app.use(bodyParser.json());
app.use(cors());
app.use(function (req, res, next) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, PATCH, DELETE');
  res.setHeader('Access-Control-Allow-Headers', 'X-Requested-With,content-type');
  res.setHeader('Access-Control-Allow-Credentials', true);
  next();
});

app.post('/dataflow/submit/batch', (req, res) => {
  dataflowSubmitBatchController.index(req, res);
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`App listening on port ${PORT}`);
  console.log('Press Ctrl+C to quit.');
});

module.exports = app;
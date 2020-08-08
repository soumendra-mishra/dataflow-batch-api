'use strict';

require("dotenv").config();
const { google } = require('googleapis');
const projectId = process.env.GCP_PROJECT_ID;

async function submitDataFlow(dataflow) {
  const stagingLocation = "gs://gcp-bucket/staging";
  const tempLocation = "gs://gcp-bucket/temp";
  const templateLocation = "gs://gcp-bucket/templates/BigQueryPipeline.json";
  const region = "us-east1";
  const dfJobName = "full-load-batch";

  const inputParams = { stagingLocation: stagingLocation, bigQueryTempLocation: tempLocation };
  const env = { tempLocation: tempLocation };
  const opts = {
    projectId: projectId,
    location: region,
    gcsPath: templateLocation,
    resource: { parameters: inputParams, environment: env, jobName: dfJobName }
  };

  const response = await dataflow.projects.locations.templates.launch(opts);
  return response;
}

async function createDataFlow() {
  return new Promise(async (resolve, reject) => {
    google.auth.getApplicationDefault(async function (err, authClient) {
      if (err) {
        console.error(err);
        throw err;
      }

      if (authClient.createScopedRequired && authClient.createScopedRequired()) {
        authClient = authClient.createScoped([
          'https://www.googleapis.com/auth/cloud-platform',
          'https://www.googleapis.com/auth/compute',
          'https://www.googleapis.com/auth/compute.readonly',
          'https://www.googleapis.com/auth/userinfo.email'
        ]);
      }

      const dataflow = google.dataflow({ version: "v1b3", auth: authClient });

      try {
        const resp = await submitDataFlow(dataflow);
        resolve(resp);
      }
      catch (err) {
        reject(err);
      };
    })
  });
}

exports.index = async function (req, res) {
  try {
    const jobStatus = await createDataFlow();
    res.status(201).send('Submitted:' + JSON.stringify(jobStatus));
  }
  catch (err) {
    console.error(err);
    res.status(500).send(err.message);
  }
};
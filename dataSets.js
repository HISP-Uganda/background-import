const schedule = require("node-schedule");
const logger = require("./Logger");
const { query2DHIS2, postDHIS2 } = require("./common");
const log = logger("data-sets");

const dataSets = [
  "NLmYDbAYwNO",
  "C4oUitImBPK",
  "onFoQ4ko74y",
  "ijQTwdoFdC3",
  "RtEYsASU7PG",
  "ic1BSWhGOso",
  "nGkMm2VBT4G",
  "VDhwrW9DiC1",
  "quMWqLxzcfO",
  "IAtaRO1bTAO",
  "V6TqjXm5sQy",
  "dFRD2A5fdvn",
  "DFMoIONIalm",
  "GwSIuQVi8b2",
  "AJPsbEM4KH7",
  "G8auYvRfXVi",
  "EBqVAQRmiPm",
  "bv6CZdughvo",
  "GcShGHoBAI0",
  "Vw6UqgPheNN",
  "zXyh8jaafWj",
  "YXQPPYkg1f2",
  "EyK1YaS3WyH",
  "JmneeJ5WLij",
  "Z04NUtAIIWc",
  "LbRczM2QQSN",
  "urGNhbeoTMV",
];

const processDataSet = async (dataSet) => {
  log.info("Fetching from hmis");
  const data = await query2DHIS2("dataValueSets.json", {
    dataSet,
    orgUnit: "akV6429SUqu",
    lastUpdatedDuration: "1h",
    children: true,
  });
  log.info(`Found ${data.dataValues.length} records`);
  log.info(`Inserting in repo`);
  const response = await postDHIS2("dataValueSets", data);
  console.log(response);
};

const dataSetJobs = schedule.scheduleJob("0 * * * *", async function () {
  for (const dataSet of dataSets) {
    log.info(`Processing dataSet ${dataSet}`);
    await processDataSet(dataSet);
  }
});

const test = async () => {
  for (const dataSet of dataSets) {
    log.info(`Processing dataSet ${dataSet}`);
    await processDataSet(dataSet);
  }
};

test().then(() => console.log("Dead"));

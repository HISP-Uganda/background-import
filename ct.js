const logger = require("./Logger");
const { query2DHIS2, postDHIS2 } = require("./common");
const log = logger("data-sets-repo");

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

const periods = [
  ["2020-01-01", "2020-12-31"],
  ["2021-01-01", "2021-12-31"],
  ["2022-01-01", "2022-12-31"],
];

const processDataSet = async (dataSet, startDate, endDate) => {
  log.info("Fetching from hmis");
  const data = await query2DHIS2("dataValueSets.json", {
    dataSet,
    orgUnit: "akV6429SUqu",
    lastUpdatedDuration: "1h",
    children: true,
    includeDeleted: true,
    startDate,
    endDate,
  });
  if (data.dataValues) {
    log.info(`Found ${data.dataValues.length} records`);
    log.info("Inserting in repo");
    const { importCount, conflicts } = await postDHIS2("dataValueSets", data);
    log.info(
      `imported: ${importCount.imported}, updated: ${importCount.updated}, ignored: ${importCount.ignored}, deleted: ${importCount.deleted}`
    );
    for (const conflict of conflicts) {
      log.warn(conflict.value);
    }
  } else {
    log.info("No records found");
  }
};

const insert = async () => {
  for (const dataSet of dataSets) {
    for (const period of periods) {
      log.info(
        `Processing dataSet ${dataSet} from ${period[0]} to ${period[1]}`
      );
      await processDataSet(dataSet, period[0], period[1]);
    }
  }
};

insert().then(() => console.log("Done"));

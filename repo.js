const { fromPairs } = require("lodash");
const { default: Axios } = require("axios");
const fs = require("fs");
const csv = require("csv-parser");
const { queryDHIS2, postDHIS2 } = require("./common");

const logger = require("./Logger");

async function downloadData(mappingDetails, id, startDate, endDate) {
  const {
    remoteDataSet,
    url,
    username,
    password,
    id: mappingId,
  } = mappingDetails;
  let remoteUrl = `${url}/api/dataValueSets.csv`;

  if (String(url).endsWith("/")) {
    remoteUrl = `${url}api/dataValueSets.csv`;
  }
  const response = await Axios({
    url: remoteUrl,
    method: "GET",
    responseType: "stream",
    auth: { username, password },
    params: {
      dataSet: remoteDataSet,
      startDate,
      endDate,
      orgUnit: id,
    },
  });
  response.data.pipe(fs.createWriteStream(`${mappingId}.csv`));
  return new Promise((resolve, reject) => {
    response.data.on("end", () => {
      resolve();
    });
    response.data.on("error", () => {
      reject();
    });
  });
}

const processFile = async (mappingId, combos, attributes, orgUnit) => {
  const dataValues = [];
  return new Promise((resolve, reject) => {
    const stream = fs.createReadStream(`${mappingId}.csv`);
    const parser = csv();
    stream.on("ready", () => {
      stream.pipe(parser);
    });
    parser.on("readable", function () {
      let row;
      while ((row = parser.read())) {
        const {
          dataelement: de,
          period,
          categoryoptioncombo: coc,
          attributeoptioncombo: aoc,
          value,
        } = row;
        const deAndCoc = combos[`${de},${coc}`];
        if (deAndCoc) {
          const attributeOptionCombo = attributes[aoc];
          const [dataElement, categoryOptionCombo] =
            String(deAndCoc).split(",");
          if (dataElement && categoryOptionCombo && attributeOptionCombo) {
            dataValues.push({
              dataElement,
              period,
              orgUnit,
              categoryOptionCombo,
              attributeOptionCombo,
              value,
            });
          }
        }
      }
    });

    parser.on("error", function (err) {
      console.error(err.message);
      reject();
    });

    parser.on("end", function () {
      resolve(dataValues);
    });
  });
};
const fetchAllMapping = async (mappingId, startDate, endDate) => {
  const log = logger(mappingId);
  const [mappingDetails, ouMapping, cMapping, aMapping] = await Promise.all([
    queryDHIS2(`dataStore/agg-wizard/${mappingId}`, {}),
    queryDHIS2(`dataStore/o-mapping/${mappingId}`, {}),
    queryDHIS2(`dataStore/c-mapping/${mappingId}`, {}),
    queryDHIS2(`dataStore/a-mapping/${mappingId}`, {}),
  ]);
  const { name } = mappingDetails;
  const combos = fromPairs(
    cMapping.filter((m) => !!m.mapping).map((m) => [m.id, m.mapping])
  );
  const attributes = fromPairs(
    aMapping.filter((m) => !!m.mapping).map((m) => [m.id, m.mapping])
  );
  let count = 1;
  const total = ouMapping.length;
  for (const { id, mapping } of ouMapping) {
    try {
      log.info(
        `Downloading data for ${count} (${mapping}) of ${total} organisation units for mapping ${name} (${mappingId})`
      );
      await downloadData(mappingDetails, id, startDate, endDate);
      log.info(
        `Processing ${count} (${mapping}) of ${total} organisation units for mapping ${name} (${mappingId})`
      );
      const dataValues = await processFile(
        mappingId,
        combos,
        attributes,
        mapping
      );
      log.info(
        `Inserting data for ${count}  of ${total} organisation units for mapping ${name} (${mappingId})`
      );
      const response = await postDHIS2("dataValueSets", { dataValues });
      log.info(JSON.stringify(response?.importCount));
      if (response.conflicts) {
        for (const conflict of response.conflicts) {
          log.warn(JSON.stringify(conflict));
        }
      }
    } catch (error) {
      log.error(error.message);
    }
    count = count + 1;
  }
};

const args = process.argv.slice(2);

if (args.length === 3) {
  fetchAllMapping(args[0], args[1], args[2]).then(() => console.log("Done"));
} else {
  console.log("Wrong arguments");
}
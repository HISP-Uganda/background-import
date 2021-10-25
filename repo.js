const { fromPairs, chunk } = require("lodash");
const { default: Axios } = require("axios");
const fs = require("fs");
const csv = require("csv-parser");
const { queryDHIS2, postDHIS2 } = require("./common");

const logger = require("./Logger");

async function downloadData(
  sectionName,
  remoteDataSet,
  url,
  username,
  password,
  id,
  startDate,
  endDate
) {
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
  response.data.pipe(fs.createWriteStream(`${sectionName}.csv`));
  return new Promise((resolve, reject) => {
    response.data.on("end", () => {
      resolve();
    });
    response.data.on("error", () => {
      reject();
    });
  });
}

const processFile = async (sectionName, combos, attributes, orgUnit) => {
  const dataValues = [];
  return new Promise((resolve, reject) => {
    const stream = fs.createReadStream(`${sectionName}.csv`);
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
const fetchAllMapping = async (
  sectionName,
  mappingIds,
  startDate,
  endDate,
  start = 0
) => {
  const log = logger(sectionName);
  const [mappingDetails, ouMapping, aMapping] = await Promise.all([
    queryDHIS2(`dataStore/agg-wizard/${mappingIds[0]}`, {}),
    queryDHIS2(`dataStore/o-mapping/${mappingIds[0]}`, {}),
    queryDHIS2(`dataStore/a-mapping/${mappingIds[0]}`, {}),
  ]);
  const { remoteDataSet, url, username, password } = mappingDetails;
  const allCombos = await Promise.all(
    mappingIds.map((mappingId) =>
      queryDHIS2(`dataStore/c-mapping/${mappingId}`, {})
    )
  );
  let combos = {};

  allCombos.forEach((combo) => {
    combos = {
      ...combos,
      ...fromPairs(
        combo.filter((m) => !!m.mapping).map((m) => [m.id, m.mapping])
      ),
    };
  });
  const attributes = fromPairs(
    aMapping.filter((m) => !!m.mapping).map((m) => [m.id, m.mapping])
  );
  let count = start + 1;
  const total = ouMapping.length;
  const units = ouMapping.slice(start);
  for (const { id, mapping } of units) {
    try {
      log.info(
        `Downloading data for ${count} (${mapping}) of ${total} organisation units for mappings (${mappingIds.join(
          ","
        )})`
      );
      await downloadData(
        sectionName,
        remoteDataSet,
        url,
        username,
        password,
        id,
        startDate,
        endDate
      );
      log.info(
        `Processing ${count} (${mapping}) of ${total} organisation units for mappings (${mappingIds.join(
          ","
        )})`
      );
      const dataValues = await processFile(
        sectionName,
        combos,
        attributes,
        mapping
      );
      log.info(`Processed ${dataValues.length} records`);
      log.info(
        `Inserting ${count}  of ${total} organisation units for mappings (${mappingIds.join(
          ","
        )})`
      );
      const response = await postDHIS2("dataValueSets", { dataValues });
      log.info(JSON.stringify(response?.importCount));
      if (response.conflicts) {
        for (const conflict of response.conflicts) {
          log.warn(`${conflict.object} ${conflict.value}`);
        }
      }
    } catch (error) {
      log.error(error.message);
    }
    count = count + 1;
  }
};

const args = process.argv.slice(2);

if (args.length >= 4) {
  const start = args.length === 5 ? parseInt(args[4], 10) : 0;
  const mappingIds = String(args[1]).split(",");
  fetchAllMapping(args[0], mappingIds, args[2], args[3], start).then(() =>
    console.log("Done")
  );
} else {
  console.log("Wrong arguments");
}

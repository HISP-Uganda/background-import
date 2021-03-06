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
  orgUnit,
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
      orgUnit,
      children: true,
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

const processFile = async (sectionName, combos, attributes, facilities) => {
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
          orgunit: unit,
          value,
        } = row;
        const deAndCoc = combos[`${de},${coc}`];
        const orgUnit = facilities[unit];
        const attributeOptionCombo = attributes[aoc];
        const [dataElement, categoryOptionCombo] = String(deAndCoc).split(",");
        if (
          deAndCoc &&
          orgUnit &&
          attributeOptionCombo &&
          dataElement &&
          categoryOptionCombo
        ) {
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

const readFile = async (sectionName) => {
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
          dataelement: dataElement,
          period,
          categoryoptioncombo: categoryOptionCombo,
          attributeoptioncombo: attributeOptionCombo,
          orgunit: orgUnit,
          value,
        } = row;
        dataValues.push({
          dataElement,
          period,
          orgUnit,
          categoryOptionCombo,
          attributeOptionCombo,
          value,
        });
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

      if (dataValues.length > 0) {
        log.info(
          `Inserting ${
            dataValues.length
          } records for ${count}  of ${total} organisation units for mappings (${mappingIds.join(
            ","
          )})`
        );
        const response = await postDHIS2(
          "dataValueSets",
          { dataValues },
          {
            async: true,
            dryRun: false,
            strategy: "NEW_AND_UPDATES",
            preheatCache: true,
            skipAudit: true,
            dataElementIdScheme: "UID",
            orgUnitIdScheme: "UID",
            idScheme: "UID",
            skipExistingCheck: false,
            format: "json",
          }
        );
        log.info(`Created task with id ${response.response.id}`);
      }
    } catch (error) {
      log.error(error.message);
    }
    count = count + 1;
  }
};

const readCSV = (fileName) => {
  const results = [];

  return new Promise((resolve, reject) => {
    fs.createReadStream(fileName)
      .pipe(csv())
      .on("data", (data) => results.push(data))
      .on("end", () => {
        resolve(results);
      });
  });
};

module.exports.fetchPerDistrict = async (
  sectionName,
  mappings,
  startDate,
  endDate,
  which = 1
) => {
  const log = logger(sectionName);
  const mappingIds = String(mappings).split(",");
  const districts = await readCSV("./organisationUnits.csv");
  const facilities = await readCSV("./facilities.csv");

  const l2Facilities = fromPairs(
    facilities.filter((facility) => !!facility.l2).map((f) => [f.l2, f.repo])
  );

  const ctFacilities = fromPairs(
    facilities.filter((facility) => !!facility.ct).map((f) => [f.ct, f.repo])
  );

  const [mappingDetails, aMapping] = await Promise.all([
    queryDHIS2(`dataStore/agg-wizard/${mappingIds[0]}`, {}),
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

  for (const district of districts) {
    // try {
    log.info(
      `Downloading data for ${
        district.displayName
      } for mappings (${mappingIds.join(",")})`
    );
    await downloadData(
      sectionName,
      remoteDataSet,
      url,
      username,
      password,
      district.id,
      startDate,
      endDate
    );
    log.info(
      `Processing data for ${
        district.displayName
      } for mappings (${mappingIds.join(",")})`
    );
    let dataValues = [];

    if (which === 1) {
      dataValues = await processFile(
        sectionName,
        combos,
        attributes,
        ctFacilities
      );
    } else {
      dataValues = await processFile(
        sectionName,
        combos,
        attributes,
        l2Facilities
      );
    }
    if (dataValues.length > 0) {
      log.info(
        `Inserting ${dataValues.length} records for ${district.displayName}`
      );
      const requests = chunk(dataValues, 50000).map((dvs) =>
        postDHIS2(
          "dataValueSets",
          { dataValues: dvs },
          {
            async: true,
            dryRun: false,
            strategy: "NEW_AND_UPDATES",
            preheatCache: true,
            skipAudit: true,
            dataElementIdScheme: "UID",
            orgUnitIdScheme: "UID",
            idScheme: "UID",
            skipExistingCheck: false,
            format: "json",
          }
        )
      );
      const responses = await Promise.all(requests);
      for (const response of responses) {
        log.info(`Created task with id ${response.response.id}`);
      }
    }
    // } catch (error) {
    //   log.error(error.message);
    // }
  }
};

const transferDDIData = async () => {
  const log = logger("DDI");

  const districts = await readCSV("./organisationUnits.csv");
  const years = [2018, 2019, 2020, 2021];

  for (const year of years) {
    const startDate = `${year}-01-01`;
    const endDate = `${year}-12-31`;
    for (const district of districts) {
      log.info(
        `Fetching data for ${district.displayName} for ${startDate} to ${endDate}`
      );
      // const {
      //   data: { dataValues },
      // } = await Axios.get(
      //   "https://repo.hispuganda.org/repo/api/dataValueSets.json",
      //   {
      //     auth: { username: "carapai", password: "Baby77@Baby771" },
      //     params: {
      //       dataSet: "mxBQbAMkGVd",
      //       startDate,
      //       endDate,
      //       orgUnit: district.id,
      //       children: true,
      //     },
      //   }
      // );
      await downloadData(
        `${district.id}-${startDate}-${endDate}`,
        "mxBQbAMkGVd",
        "https://hmis-repo.health.go.ug/",
        "carapai",
        "Baby77@Baby771",
        district.id,
        startDate,
        endDate
      );
      // const dataValues = await readFile("DDI");

      // if (dataValues && dataValues.length > 0) {
      //   log.info(
      //     `Found ${dataValues.length} values for ${district.displayName} for ${startDate} to ${endDate}`
      //   );
      //   try {
      //     const requests = chunk(dataValues, 50000).map((dvs) =>
      //       postDHIS2(
      //         "dataValueSets",
      //         { dataValues: dvs },
      //         {
      //           async: true,
      //           dryRun: false,
      //           strategy: "NEW_AND_UPDATES",
      //           preheatCache: true,
      //           skipAudit: true,
      //           dataElementIdScheme: "UID",
      //           orgUnitIdScheme: "UID",
      //           idScheme: "UID",
      //           skipExistingCheck: false,
      //           format: "json",
      //         }
      //       )
      //     );
      //     const responses = await Promise.all(requests);
      //     for (const response of responses) {
      //       log.info(`Created task with id ${response.response.id}`);
      //     }
      //   } catch (error) {}
      // }
    }
  }
};
const args = process.argv.slice(2);

if (args.length >= 2) {
  const which = args.length === 5 ? 1 : 2;
  this.fetchPerDistrict(args[0], args[1], args[2], args[3], which).then(() =>
    console.log("Done")
  );
} else {
  console.log("Wrong arguments");
}

// transferDDIData();

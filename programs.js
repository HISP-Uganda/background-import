const schedule = require("node-schedule");
const logger = require("./Logger");
const { query2DHIS2, postDHIS2 } = require("./common");
const log = logger("programs");
const trackers = [
  "qLXolImKO4p",
  "DysyMchxKuu",
  "h4n7b1HjSnX",
  "NPzEcFPfQWF",
  "oBjNGwbT37L",
];
const events = [
  "M9iSvQgzYVM",
  "vf8dN49jprI",
  "zQZss0QIRq4",
  "ZJRDIb1joXP",
  "HjkGOlFiGij",
  "gMC8hUMD4Zi",
  "k7i7qPFQV5n",
];

const processEvents = async (program, pageSize = 500) => {
  const params = {
    program,
    ouMode: "ALL",
    lastUpdatedDuration: "5m",
    page: 1,
    pageSize,
  };
  log.info(`Querying page 1 from hmis`);
  const {
    events,
    pager: { pageCount },
  } = await await query2DHIS2("events.json", params);
  log.info(`Posting page 1 to repo`);
  await postDHIS2("events", { events });

  if (pageCount > 1) {
    for (let page = 2; page <= pageCount; page++) {
      log.info(`Querying page ${page} of ${pageCount} from hmis`);
      const {
        events,
        pager: { pageCount },
      } = await await query2DHIS2("events.json", { ...params, page });
      log.info(`Posting page ${page} of ${pageCount} to repo`);
      await postDHIS2("events", { events });
    }
  }
};

const processTracker = async (program, pageSize = 100) => {
  const params = {
    program,
    ouMode: "ALL",
    lastUpdatedDuration: "5m",
    page: 1,
    pageSize,
    fields: "*",
  };
  log.info(`Querying page 1 from hmis`);
  const {
    trackedEntityInstances,
    pager: { pageCount },
  } = await await query2DHIS2("trackedEntityInstances.json", params);
  log.info(`Posting page 1 to repo`);
  await postDHIS2("trackedEntityInstances", { trackedEntityInstances });
  if (pageCount > 1) {
    for (let page = 2; page <= pageCount; page++) {
      log.info(`Querying page ${page} of ${pageCount} from hmis`);
      const {
        trackedEntityInstances,
        pager: { pageCount },
      } = await await query2DHIS2("trackedEntityInstances.json", {
        ...params,
        page,
      });
      log.info(`Posting page ${page} of ${pageCount} to repo`);
      await postDHIS2("trackedEntityInstances", { trackedEntityInstances });
    }
  }
};

const programJob = schedule.scheduleJob("*/5 * * * *", async function () {
  for (const program of events) {
    await processEvents(program);
  }

  for (const program of trackers) {
    await processTracker(program);
  }
});

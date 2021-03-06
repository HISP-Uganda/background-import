const { search, bulk } = require("./elasticsearch");
const { fromPairs, flatten, uniq, groupBy } = require("lodash");
const mergeByKey = require("array-merge-by-key");
const axios = require("axios");
const fs = require("fs").promises;
const {
  differenceInDays,
  differenceInMinutes,
  differenceInSeconds,
} = require("date-fns");
const defenceSites = require("./defenceSites.json");
const logger = require("./Logger");

const PROGRAM = "yDuAzyqYABS";
const NIN_ATTRIBUTE = "Ewi7FUfcHAD";
const PROGRAM_STAGE = "a1jCssI2LkW";
const OTHER_ID = "YvnFn4IjKzx";

let defenceUnits = fromPairs(defenceSites.map((ou) => [ou.id, ou.name]));

const log = logger("epivac");

module.exports.interval = (startDate, endDate) => {
  const days = differenceInDays(endDate, startDate);
  if (days === 0) {
    const minutes = differenceInMinutes(endDate, startDate);
    if (minutes === 0) {
      return `${differenceInSeconds(endDate, startDate)}s`;
    }
    return `${minutes}m`;
  }
  return `${days}d`;
};

module.exports.createApi = (baseURL, username, password) => {
  return axios.create({
    baseURL,
    auth: {
      username,
      password,
    },
  });
};

module.exports.processInstance = async (params) => {
  const { attributes, enrollments, trackedEntityInstance } = params;
  const enroll = enrollments.filter((en) => en.program === PROGRAM);
  const allEvents = enroll.flatMap((en) => en.events);
  let results = fromPairs(attributes.map((a) => [a.attribute, a.value]));
  const id = `${results[NIN_ATTRIBUTE] || ""}${results[OTHER_ID] || ""}`;
  let processedEvents = allEvents
    .filter(
      (event) =>
        !!event.eventDate &&
        event.deleted === false &&
        event.programStage === PROGRAM_STAGE
    )
    .map(({ dataValues, relationships, notes, ...others }) => {
      return {
        ...others,
        ...fromPairs(dataValues.map((dv) => [dv.dataElement, dv.value])),
      };
    });
  const allFacilities = uniq(processedEvents.map((e) => e.orgUnit));

  const facilities = await Promise.all(
    allFacilities.map((id) => {
      return search({
        index: "facilities",
        body: {
          query: {
            match: { id },
          },
        },
      });
    })
  );

  let foundFacilities = fromPairs(
    facilities
      .map(([data]) => {
        if (data && data._source) {
          const {
            _source: { id, ...rest },
          } = data;
          return [id, { id, ...rest }];
        }
        return null;
      })
      .filter((d) => d !== null)
  );

  processedEvents = processedEvents.map((event) => {
    const facility = foundFacilities[event.orgUnit] || {};
    let newEvent = { ...event, ...facility };
    const siteChange = defenceUnits[event.orgUnit];
    if (siteChange) {
      newEvent = {
        ...newEvent,
        name: siteChange,
        orgUnitName: siteChange,
      };
    }
    return newEvent;
  });
  const groupedData = groupBy(processedEvents, "LUIsbsm3okG");
  const pp = Object.entries(groupedData).map(([dose, allDoses]) => {
    const gotDoses = mergeByKey("LUIsbsm3okG", allDoses);
    return [dose, gotDoses.length > 0 ? gotDoses[0] : {}];
  });
  return {
    ...results,
    certificate: Math.floor(
      Math.random() * (99999999 - 10000000 + 1) + 10000000
    ),
    trackedEntityInstance,
    id,
    ...fromPairs(pp),
  };
};

module.exports.processInstances = async (params) => {
  const { trackedEntityInstances } = params;
  let instances = [];
  for (const instance of trackedEntityInstances) {
    let currentData = await this.processInstance(instance);
    if (currentData.id) {
      const previous = await search({
        index: "certificates",
        body: {
          query: {
            match: { id: currentData.id },
          },
        },
      });
      if (previous.length > 0) {
        const previousData = previous[0]._source;
        currentData = {
          ...previousData,
          ...currentData,
          certificate: previousData.certificate
            ? previousData.certificate
            : currentData.certificate,
        };
      }
      instances.push(currentData);
    }
  }
  return await bulk({
    index: "certificates",
    dataset: instances,
    id: "id",
  });
};

module.exports.fetchEvents = async (
  baseURL,
  username,
  password,
  lastUpdatedDuration = null
) => {
  const api = this.createApi(baseURL, username, password);
  let params = {
    program: PROGRAM,
    ouMode: "ALL",
    totalPages: true,
    page: 1,
    fields: "trackedEntityInstance",
    pageSize: 50,
  };

  if (lastUpdatedDuration) {
    log.info(`Adding last updated param of ${lastUpdatedDuration}`);
    params = {
      ...params,
      lastUpdatedDuration,
    };
  }
  log.info(`Fetching initial data`);
  const {
    data: {
      events,
      pager: { pageCount },
    },
  } = await api.get("events.json", {
    params,
  });
  const processed = events
    .map((e) => {
      return e.trackedEntityInstance;
    })
    .join(";");

  const {
    data: { trackedEntityInstances },
  } = await api.get("trackedEntityInstances.json", {
    params: {
      program: PROGRAM,
      ouMode: "ALL",
      fields: "*",
      trackedEntityInstance: processed,
    },
  });

  await this.processInstances({
    trackedEntityInstances,
  });

  log.info(`Processing initial data`);
  await this.processInstances({
    trackedEntityInstances,
  });
  if (pageCount > 1) {
    for (let page = 2; page <= pageCount; page++) {
      log.info(`Fetching ${page} of ${pageCount}`);
      const {
        data: { events },
      } = await api.get("events.json", {
        params: { ...params, page },
      });
      log.info(`Processing ${page} of ${pageCount}`);
      const processed = events
        .map((e) => {
          return e.trackedEntityInstance;
        })
        .join(";");

      const {
        data: { trackedEntityInstances },
      } = await api.get("trackedEntityInstances.json", {
        params: {
          program: PROGRAM,
          ouMode: "ALL",
          fields: "*",
          trackedEntityInstance: processed,
        },
      });
      await this.processInstances({
        trackedEntityInstances,
      });
    }
  }
};

module.exports.epivac = async (
  baseURL,
  username,
  password,
  lastUpdatedDuration = null
) => {
  const api = this.createApi(baseURL, username, password);
  let params = {
    program: PROGRAM,
    ouMode: "ALL",
    totalPages: true,
    page: 1,
    fields: "*",
    pageSize: 1000,
  };

  if (lastUpdatedDuration) {
    log.info(`Adding last updated param of ${lastUpdatedDuration}`);
    params = {
      ...params,
      lastUpdatedDuration,
    };
  }
  log.info(`Fetching initial data`);
  const {
    data: {
      trackedEntityInstances,
      pager: { pageCount },
    },
  } = await api.get("trackedEntityInstances.json", {
    params,
  });

  log.info(`Processing initial data`);
  await this.processInstances({
    trackedEntityInstances,
  });
  if (pageCount > 1) {
    for (let page = 2; page <= pageCount; page++) {
      log.info(`Fetching ${page} of ${pageCount}`);
      const {
        data: { trackedEntityInstances },
      } = await api.get("trackedEntityInstances.json", {
        params: { ...params, page },
      });
      log.info(`Processing ${page} of ${pageCount}`);
      await this.processInstances({
        trackedEntityInstances,
      });
    }
  }
  log.info(`Finished processing`);
};
module.exports.processFacilities = (organisationUnits) => {
  return organisationUnits.map((unit) => {
    const { id, name } = unit;
    let facility = {
      id,
      name,
    };

    if (unit.parent) {
      const subCounty = unit.parent;
      facility = {
        ...facility,
        subCountyId: subCounty.id,
        subCountyName: subCounty.name,
      };
      if (subCounty.parent) {
        const district = subCounty.parent;
        facility = {
          ...facility,
          districtId: district.id,
          districtName: district.name,
        };

        if (district.parent) {
          const region = district.parent;
          facility = {
            ...facility,
            regionId: region.id,
            regionName: region.name,
          };

          if (region.parent) {
            const country = region.parent;
            facility = {
              ...facility,
              countryId: country.id,
              countryName: country.name,
            };
          }
        }
      }
    }
    return facility;
  });
};

module.exports.syncFacilities = async (baseURL, username, password) => {
  const api = this.createApi(baseURL, username, password);
  const params = {
    fields:
      "organisationUnits[id,name,parent[id,name,parent[id,name,parent[id,name,parent[id,name]]]]]",
  };
  const {
    data: { organisationUnits },
  } = await api.get(`programs/yDuAzyqYABS`, { params });
  const processed = this.processFacilities(organisationUnits);
  const { inserted } = await bulk({
    index: "facilities",
    dataset: processed,
    id: "id",
  });
  log.info(`Inserted ${inserted}`);
};

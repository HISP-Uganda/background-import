const { groupBy, chunk } = require("lodash");
const fs = require("fs");
const XLSX = require("xlsx");
const csv = require("csv-parser");
const stringSimilarity = require("string-similarity");
const hvatAttributes = require("./hvatAttributes.json");
const hvatDataElements = require("./hvatDataElements.json");
const { queryDHIS2, postDHIS2, validText, validateValue } = require("./common");

const processInstances = (trackedEntityInstances, identifierIds) => {
  const instances = {};
  trackedEntityInstances.forEach((instance) => {
    for (const identifierId of identifierIds) {
      const attribute = instance.attributes.find(
        (a) => a.attribute === identifierId
      );
      if (attribute) {
        instances[attribute.value] = instance;
      }
    }
  });
  return instances;
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

const fetchPrevious = async (district, program, identifierId) => {
  //   const pages = 47;
  //   let allInstances = {};
  const {
    trackedEntityInstances,
    pager: { pageCount },
  } = await queryDHIS2("trackedEntityInstances.json", {
    ouMode: "ALL",
    // ou: district,
    program,
    page: 1,
    fields: "*",
    pageSize: 1000,

    totalPages: true,
  });
  const page1 = processInstances(trackedEntityInstances, identifierId);
  let allInstances = page1;
  fs.writeFileSync(`page1.json`, JSON.stringify(page1));
  for (let page = 2; page <= pageCount; page++) {
    console.log(`Fetching page ${page} of ${pageCount}`);
    const { trackedEntityInstances } = await queryDHIS2(
      "trackedEntityInstances.json",
      {
        ouMode: "ALL",
        program,
        fields: "*",
        page,
        pageSize: 1000,
      }
    );
    const currentInstances = processInstances(
      trackedEntityInstances,
      identifierId
    );
    allInstances = {
      ...allInstances,
      ...currentInstances,
    };
    fs.writeFileSync(`page${page}.json`, JSON.stringify(currentInstances));
  }

  //   for (let index = 1; index <= pages; index++) {
  //     const currentData = require(`./page${index}.json`);
  //     allInstances = { ...allInstances, ...currentData };
  //   }
  return allInstances;
};

const processData = async (
  fileName,
  trackedEntityType,
  district,
  program,
  identifierId,
  attributeIds,
  programStage = "",
  dataElementIds = []
) => {
  const facilities = await readCSV("./units.csv");
  const previous = await fetchPrevious(district, program, identifierId);
  const bySubConties = groupBy(facilities, "subcountyCode");
  const byParishes = groupBy(
    facilities,
    (f) =>
      `${f.subcountyId}${String(f.name)
        .toLowerCase()
        .replace("parish", "")
        .replaceAll(" ", "")}`
  );
  const bySubcountyNames = groupBy(
    facilities,
    (f) =>
      `${f.districtId}${String(f.subcountyName)
        .toLowerCase()
        .replaceAll(" ", "")}`
  );
  const parishes = Object.keys(byParishes);
  const currentData = await readCSV(fileName);
  const allTrackedEntityInstances = [];
  const newTrackedEntityInstances = [];
  const missingSubCounties = [];
  const missingParishes = [];

  for (let d of currentData) {
    const currentInstance =
      previous[d["qREMYWEohY6"]] || previous[d["r10igcWrpoH"]];
    if (currentInstance) {
      let attributes = currentInstance.attributes.map((attribute) => {
        if (attribute.attribute === "qREMYWEohY6") {
          return { ...attribute, value: d["qREMYWEohY6"] };
        }
        if (attribute.attribute === "r10igcWrpoH") {
          return { ...attribute, value: d["r10igcWrpoH"] };
        }
        if (attribute.attribute === "DFm9VvEp91p") {
          return {
            ...attribute,
            value: String(d["DFm9VvEp91p"]).padStart(4, "0"),
          };
        }
        return attribute;
      });
      const oldAttribute = attributes.find(
        (a) => a && a.attribute === "qREMYWEohY6"
      );

      if (!oldAttribute) {
        attributes = [
          ...attributes,
          { attribute: "qREMYWEohY6", value: d["qREMYWEohY6"] },
        ];
      }
      allTrackedEntityInstances.push({ ...currentInstance, attributes });
    } else {
      const subCountyString = `${d["districtId"]}${String(
        d["Sub-County/Division/ Town Council"]
      )
        .toLowerCase()
        .replaceAll(" ", "")}`;
      const subCounty =
        bySubcountyNames[subCountyString] || bySubConties[d.subCounty];
      if (subCounty) {
        d = { ...d, subCountyId: subCounty[0].subcountyId };
        const subCountyId = subCounty[0].subcountyId;
        const parishString = `${subCountyId}${String(d["Parish/ Ward"])
          .toLowerCase()
          .replace(" ", "")}`;
        const parish = byParishes[parishString];

        const percentages = stringSimilarity.findBestMatch(
          parishString,
          parishes
        );
        let parishId;
        if (parish) {
          parishId = parish[0].id;
        } else if (
          String(percentages.bestMatch.target).indexOf(parishString) !== -1
        ) {
          parishId = byParishes[percentages.bestMatch.target][0].id;
        }
        if (parishId) {
          const currentAttributes = attributeIds.flatMap((a) => {
            const validatedValue = validateValue(
              a.trackedEntityAttribute.valueType,
              d[a.trackedEntityAttribute.id],
              a.trackedEntityAttribute.optionSetValue,
              a.trackedEntityAttribute.optionSet
            );
            if (validatedValue !== null) {
              if (a.trackedEntityAttribute.id === "DFm9VvEp91p") {
                return [
                  {
                    attribute: a.trackedEntityAttribute.id,
                    value: String(validatedValue).padStart(4, "0"),
                  },
                ];
              }
              return [
                {
                  attribute: a.trackedEntityAttribute.id,
                  value: validatedValue,
                },
              ];
            }

            return [];
          });
          const dataValues = dataElementIds.flatMap((e) => {
            const validatedValue = validateValue(
              e.dataElement.valueType,
              d[e.dataElement.id],
              e.dataElement.optionSetValue,
              e.dataElement.optionSet
            );
            if (validatedValue !== null) {
              return [{ dataElement: e.dataElement.id, value: validatedValue }];
            }
            return [];
          });

          const trackedEntityInstance = {
            orgUnit: parishId,
            trackedEntityType,
            attributes: currentAttributes,
            enrollments: [
              {
                enrollmentDate: d["Date"],
                program,
                incidentDate: d["Date"],
                orgUnit: parishId,
                events: [
                  {
                    eventDate: d["Date"],
                    programStage,
                    program,
                    orgUnit: parishId,
                    dataValues,
                  },
                ],
              },
            ],
          };
          newTrackedEntityInstances.push(trackedEntityInstance);
        } else {
          missingParishes.push(d);
        }
      } else {
        missingSubCounties.push(d);
      }
    }
  }

  const ws = XLSX.utils.json_to_sheet(missingSubCounties);
  const wsParishes = XLSX.utils.json_to_sheet(missingParishes);

  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, "missing_subcounties");
  XLSX.utils.book_append_sheet(wb, wsParishes, "missing_parishes");
  XLSX.writeFile(wb, `${district}.xlsx`);

  let c = 1;
  const allChunks = chunk(allTrackedEntityInstances, 250);

  for (const trackedEntityInstances of allChunks) {
    try {
      console.log(`Updating page ${c} of ${allChunks.length}`);
      const response = await postDHIS2("trackedEntityInstances", {
        trackedEntityInstances,
      });
      c = c + 1;
    } catch (error) {
      console.log(error.message);
    }
  }

  let cn = 1;

  const newChunks = chunk(newTrackedEntityInstances, 250);
  for (const trackedEntityInstances of newChunks) {
    try {
      console.log(`posting page ${cn} of ${newChunks.length}`);
      const response = await postDHIS2("trackedEntityInstances", {
        trackedEntityInstances,
      });
      cn = cn + 1;
    } catch (error) {
      console.log(error);
    }
  }
};

processData(
  "./hvat-final.csv",
  "SXQEqYXKejK",
  "aIahLLmtvgT",
  "HEWq6yr4cs5",
  "r10igcWrpoH",
  hvatAttributes,
  "sYE3K7fFM4Y",
  hvatDataElements
).then(() => console.log("Done"));

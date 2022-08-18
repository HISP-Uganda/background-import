const logger = require("./Logger");
const {postDHIS2, readCSV, downloadData, processFile} = require("./common");
const {chunk, range} = require("lodash");
const {default: Axios} = require("axios");
const {eachMonthOfInterval, eachQuarterOfInterval, eachWeekOfInterval, parseISO, format} = require("date-fns")
const log = logger("data-sets-repo");

const original = [
	"2019July",
	"2020July",
	"2021July",
	"2022July",
]
const quarterly = (start, end) => {
	const startDate = new Date(start);
	const endDate = new Date(end);
	return eachQuarterOfInterval({start: startDate, end: endDate}).map((d) => format(d, "yyyy'Q'Q"));
}

const months2 = (start, end) => {
	const startDate = new Date(start);
	const endDate = new Date(end);
	return eachMonthOfInterval({start: startDate, end: endDate}).map((d) => format(d, "yyyyMM"))
}

const weeks = (start, end) => {
	const startDate = new Date(start);
	const endDate = new Date(end);
	return eachWeekOfInterval({start: startDate, end: endDate}).map((d) => format(d, "yyyy'W'I"))
}

const dataSets = [
	{
		name: "_Estimates: Projected Population Estimates By Age",
		id: "NLmYDbAYwNO",
		periodType: months2("2020-01-02", "2022-12-31")
	},
	{
		name: "HMIS 033b - Weekly Epidemiological Surveillance Report",
		id: "C4oUitImBPK",
		periodType: weeks("2020-01-02", "2022-12-31")
	},
	{
		name: "HMIS 097b - VHT/ICCM Quarterly Report",
		id: "onFoQ4ko74y",
		periodType: quarterly("2020-01-01", "2022-12-31")
	},
	{
		name: "HMIS 104 - NTDS MDA Implementation Report",
		id: "ijQTwdoFdC3",
		periodType: months2("2020-01-02", "2022-12-31")
	},
	{
		name: "HMIS 105:01 - OPD Monthly Report (Attendance, Referrals, Conditions,TB, Nutrition)",
		id: "RtEYsASU7PG",
		periodType: months2("2020-01-02", "2022-12-31")
	},
	{
		name: "HMIS 105:02-03 - OPD Monthly Report (MCH, FP, EID, EPI & HEPB)",
		id: "ic1BSWhGOso",
		periodType: months2("2020-01-02", "2022-12-31")
	},
	{
		name: "HMIS 105:04-05 - OPD Monthly Report (HTS & SMC)",
		id: "nGkMm2VBT4G",
		periodType: months2("2020-01-02", "2022-12-31")
	},
	{
		name: "HMIS 105:06-09 - OPD Monthly Report (Supplies, Outreaches & Supervision)",
		id: "VDhwrW9DiC1",
		periodType: months2("2020-01-02", "2022-12-31")
	},
	{
		name: "HMIS 105:10 - OPD Monthly Report (Lab)",
		id: "quMWqLxzcfO",
		periodType: months2("2020-01-02", "2022-12-31")
	},
	{
		name: "HMIS 105a - Specialized Outpatient monthly report",
		id: "IAtaRO1bTAO",
		periodType: months2("2020-01-02", "2022-12-31")
	},
	{
		name: "HMIS 105C- Palliative Care Monthly Report",
		id: "V6TqjXm5sQy",
		periodType: months2("2020-01-02", "2022-12-31")
	},
	{
		name: "HMIS 106a:01-02 - HIV Quarterly Report",
		id: "dFRD2A5fdvn",
		periodType: quarterly("2020-01-01", "2022-12-31")
	},
	{
		name: "HMIS 106a:03 - TB/Leprosy Quarterly Report",
		id: "DFMoIONIalm",
		periodType: quarterly("2020-01-01", "2022-12-31")
	},
	{
		name: "HMIS 106a:04 - Lab Quarterly Report",
		id: "GwSIuQVi8b2",
		periodType: quarterly("2020-01-01", "2022-12-31")
	},
	{
		name: "HMIS 107a - Subcounty Annual Population Projection Report",
		id: "atRhxzpCbC1",
		periodType: original
	},
	{
		name: "HMIS 107c - Health Facility Human Resource Inventory",
		id: "A9MFd92O3GZ",
		periodType: original
	},
	{
		name: "HMIS 107 -  Health Unit Population and Annual Report             ",
		id: "qwSAQ1CIkjp",
		periodType: original
	},
	{
		name: "HMIS 108a - Specialised Inpatient Monthly Report",
		id: "AJPsbEM4KH7",
		periodType: months2("2020-01-02", "2022-12-31")
	},
	{
		name: "_HMIS 108a - Specialised Inpatient Monthly Report-original",
		id: "G8auYvRfXVi",
		periodType: months2("2020-01-02", "2022-12-31")
	},
	{
		name: "HMIS 108 - IPD Monthly Report",
		id: "EBqVAQRmiPm",
		periodType: months2("2020-01-02", "2022-12-31")
	},
	{
		name: "_HMIS 108 - IPD Monthly Report_Clone",
		id: "X5XpcuXLyV2",
		periodType: months2("2020-01-02", "2022-12-31")
	},
	{
		name: "HMIS 110 - District Health Facility  WASH Report",
		id: "bv6CZdughvo",
		periodType: original
	},
	{
		name: "HMIS 111 - District School WASH Summary Report",
		id: "GcShGHoBAI0",
		periodType: original
	},
	{
		name: "HMIS 127b - Visceral Leishmaniasis Inpatient Monthly Report",
		id: "Vw6UqgPheNN",
		periodType: months2("2020-01-02", "2022-12-31")
	},
	{
		name: "HMIS 128b - Visceral Leishmaniasis Outpatient Monthly Report",
		id: "zXyh8jaafWj",
		periodType: months2("2020-01-02", "2022-12-31")
	},
	{
		name: "HMIS MAL 002 - Malaria Vector Monthly Reporting Form",
		id: "YXQPPYkg1f2",
		periodType: months2("2020-01-02", "2022-12-31")
	},
	{
		name: "LMIS: Test Kits Report",
		id: "EyK1YaS3WyH",
		periodType: months2("2020-01-02", "2022-12-31")
	},
	{
		name: "LMIS: TWOS - TB First Line Report",
		id: "JmneeJ5WLij",
		periodType: months2("2020-01-02", "2022-12-31")
	},
	{
		name: "LMIS: TWOS TB MDR Report",
		id: "Z04NUtAIIWc",
		periodType: months2("2020-01-02", "2022-12-31")
	},
	{
		name: "LMIS: WAOS  Report",
		id: "LbRczM2QQSN",
		periodType: months2("2020-01-02", "2022-12-31")
	},
	{
		name: "Targets : Estimated TB cases and TB Targets",
		id: "urGNhbeoTMV",
		periodType: quarterly("2020-01-01", "2022-12-31")
	}
];
const processDataSet = async (dataSet, orgUnit, period) => {
	log.info("Downloading from hmis");
	await downloadData([dataSet], orgUnit, period);
	const dataValues = await processFile(dataSet);
	if (dataValues && dataValues.length > 0) {
		log.info(`Found ${dataValues.length} records`);
		log.info("Inserting in repo");
		const requests = chunk(dataValues, 50000).map((dvs) =>
			postDHIS2(
				"dataValueSets",
				{dataValues: dvs},
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
			log.info(`Created task with url https://hmis-repo.health.go.ug${response.response["relativeNotifierEndpoint"]}`);
			log.info(`Check status at https://hmis-repo.health.go.ug/api/system/taskSummaries/DATAVALUE_IMPORT/${response.response.id}.json`);
		}
	} else {
		log.info("No records found");
	}
};

const insert = async () => {
	const districts = await readCSV("./organisationUnits.csv");
	for (const found of chunk(districts, 5)) {
		const orgUnit = found.map(({id}) => id);
		const displayName = found.map(({displayName}) => displayName).join(",")
		for (const {id, name, periodType} of dataSets) {
			let periods = [periodType]
			if (periodType.length > 12) {
				periods = chunk(periodType, 12);
			} else if (periodType.length > 4) {
				periods = chunk(periodType, 3)
			}
			for (const period of periods) {
				log.info(
					`Processing dataSet ${name} for ${displayName} for period ${period.join(",")}`
				);
				await processDataSet([id], orgUnit, period);
			}
		}
	}
};

insert().then(() => console.log("Done"))

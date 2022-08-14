const logger = require("./Logger");
const {query2DHIS2, postDHIS2, readCSV} = require("./common");
const {chunk} = require("lodash");
const log = logger("data-sets-repo");

const dataSets = [
	{
		name: "_Estimates: Projected Population Estimates By Age",
		id: "NLmYDbAYwNO",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	},
	{
		name: "HMIS 033b - Weekly Epidemiological Surveillance Report",
		id: "C4oUitImBPK",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	},
	{
		name: "HMIS 097b - VHT/ICCM Quarterly Report",
		id: "onFoQ4ko74y",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	},
	{
		name: "HMIS 104 - NTDS MDA Implementation Report",
		id: "ijQTwdoFdC3",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	},
	{
		name: "HMIS 105:01 - OPD Monthly Report (Attendance, Referrals, Conditions,TB, Nutrition)",
		id: "RtEYsASU7PG",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	},
	{
		name: "HMIS 105:02-03 - OPD Monthly Report (MCH, FP, EID, EPI & HEPB)",
		id: "ic1BSWhGOso",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	},
	{
		name: "HMIS 105:04-05 - OPD Monthly Report (HTS & SMC)",
		id: "nGkMm2VBT4G",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	},
	{
		name: "HMIS 105:06-09 - OPD Monthly Report (Supplies, Outreaches & Supervision)",
		id: "VDhwrW9DiC1",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	},
	{
		name: "HMIS 105:10 - OPD Monthly Report (Lab)",
		id: "quMWqLxzcfO",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	},
	{
		name: "HMIS 105a - Specialized Outpatient monthly report",
		id: "IAtaRO1bTAO",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	},
	{
		name: "HMIS 105C- Palliative Care Monthly Report",
		id: "V6TqjXm5sQy",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	},
	{
		name: "HMIS 106a:01-02 - HIV Quarterly Report",
		id: "dFRD2A5fdvn",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	},
	{
		name: "HMIS 106a:03 - TB/Leprosy Quarterly Report",
		id: "DFMoIONIalm",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	},
	{
		name: "HMIS 106a:04 - Lab Quarterly Report",
		id: "GwSIuQVi8b2",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	},
	{
		name: "HMIS 107a - Subcounty Annual Population Projection Report",
		id: "atRhxzpCbC1",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	},
	{
		name: "HMIS 107c - Health Facility Human Resource Inventory",
		id: "A9MFd92O3GZ",
		periodType: [
			["2019-07-01", "2020-06-30"],
			["2020-07-01", "2021-06-30"],
			["2021-07-01", "2022-06-30"],
		]
	},
	{
		name: "HMIS 107 -  Health Unit Population and Annual Report             ",
		id: "qwSAQ1CIkjp",
		periodType: [
			["2019-07-01", "2020-06-30"],
			["2020-07-01", "2021-06-30"],
			["2021-07-01", "2022-06-30"],
		]
	},
	{
		name: "HMIS 108a - Specialised Inpatient Monthly Report",
		id: "AJPsbEM4KH7",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	},
	{
		name: "_HMIS 108a - Specialised Inpatient Monthly Report-original",
		id: "G8auYvRfXVi",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	},
	{
		name: "HMIS 108 - IPD Monthly Report",
		id: "EBqVAQRmiPm",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	},
	{
		name: "_HMIS 108 - IPD Monthly Report_Clone",
		id: "X5XpcuXLyV2",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	},
	{
		name: "HMIS 110 - District Health Facility  WASH Report",
		id: "bv6CZdughvo",
		periodType: [
			["2019-07-01", "2020-06-30"],
			["2020-07-01", "2021-06-30"],
			["2021-07-01", "2022-06-30"],
		]
	},
	{
		name: "HMIS 111 - District School WASH Summary Report",
		id: "GcShGHoBAI0",
		periodType: [
			["2019-07-01", "2020-06-30"],
			["2020-07-01", "2021-06-30"],
			["2021-07-01", "2022-06-30"],
		]
	},
	{
		name: "HMIS 127b - Visceral Leishmaniasis Inpatient Monthly Report",
		id: "Vw6UqgPheNN",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	},
	{
		name: "HMIS 128b - Visceral Leishmaniasis Outpatient Monthly Report",
		id: "zXyh8jaafWj",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	},
	{
		name: "HMIS MAL 002 - Malaria Vector Monthly Reporting Form",
		id: "YXQPPYkg1f2",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	},
	{
		name: "LMIS: Test Kits Report",
		id: "EyK1YaS3WyH",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	},
	{
		name: "LMIS: TWOS - TB First Line Report",
		id: "JmneeJ5WLij",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	},
	{
		name: "LMIS: TWOS TB MDR Report",
		id: "Z04NUtAIIWc",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	},
	{
		name: "LMIS: WAOS  Report",
		id: "LbRczM2QQSN",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	},
	{
		name: "Targets : Estimated TB cases and TB Targets",
		id: "urGNhbeoTMV",
		periodType: [
			["2020-01-01", "2020-12-31"],
			["2021-01-01", "2021-12-31"],
			["2022-01-01", "2022-12-31"],
		]
	}
];
const processDataSet = async (dataSet, orgUnit, startDate, endDate) => {
	log.info("Fetching from hmis");
	const {dataValues} = await query2DHIS2("dataValueSets.json", {
		dataSet,
		orgUnit,
		children: true,
		includeDeleted: true,
		startDate,
		endDate,
	});
	if (dataValues) {
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
	for (const {id: orgUnit, displayName} of districts) {
		for (const {id, name, periodType} of dataSets) {
			for (const period of periodType) {
				log.info(
					`Processing dataSet ${name} for ${displayName} from ${period[0]} to ${period[1]}`
				);
				await processDataSet(id, orgUnit, period[0], period[1]);
			}
		}
	}
};

insert().then(() => console.log("Done"));

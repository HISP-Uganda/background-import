const schedule = require("node-schedule");
const fs = require("fs");
const logger = require("./Logger");
const {query2DHIS2, postDHIS2, sendMail} = require("./common");
const {differenceInMinutes, parseISO} = require("date-fns");
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
	let searches = JSON.parse(fs.readFileSync("./data-sets.json", "utf8"));

	let lastUpdatedDuration = "1m";

	if (searches.last) {
		const minutes = differenceInMinutes(new Date(), parseISO(searches.last));
		if (minutes > 0 && minutes < 60) {
			lastUpdatedDuration = `${minutes}m`;
		} else if (minutes >= 60 && minutes <= 60 * 24) {
			lastUpdatedDuration = `${Math.floor(minutes / 60)}h`;
		} else if (minutes > 60 * 24) {
			lastUpdatedDuration = `${Math.floor(minutes / (60 * 24))}d`;
		}
	}

	log.info("Fetching from hmis");
	const data = await query2DHIS2("dataValueSets.json", {
		dataSet,
		orgUnit: "akV6429SUqu",
		lastUpdatedDuration,
		children: true,
		includeDeleted: true
	});
	if (data.dataValues) {
		log.info(`Found ${data.dataValues.length} records`);
		log.info("Inserting in repo");
		const {importCount, conflicts} = await postDHIS2("dataValueSets", data);
		log.info(
			`imported: ${importCount.imported}, updated: ${importCount.updated}, ignored: ${importCount.ignored}, deleted: ${importCount.deleted}`
		);
		for (const conflict of conflicts) {
			log.warn(conflict.value);
		}

	} else {
		log.info("No records found");
	}

	fs.writeFileSync(
		"./data-sets.json",
		JSON.stringify({...searches, last: new Date()})
	);
};

schedule.scheduleJob("*/5 * * * *", async function () {
	for (const dataSet of dataSets) {
		log.info(`Processing dataSet ${dataSet}`);
		await processDataSet(dataSet);
	}
});

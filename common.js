const axios = require("axios");
const nodemailer = require("nodemailer");
const csv = require("csv-parser");
const fs = require("fs");
const FormData = require("form-data");
const dotenv = require("dotenv");
const {default: Axios} = require("axios");

const result = dotenv.config();

if (result.error) {
	throw result.error;
}
module.exports.getDHIS2Url1 = (uri) => {
	if (uri !== "") {
		try {
			const url = new URL(uri);
			const dataURL = url.pathname.split("/");
			const apiIndex = dataURL.indexOf("api");

			if (apiIndex !== -1) {
				return url.href;
			} else {
				if (dataURL[dataURL.length - 1] === "") {
					return url.href + "api";
				} else {
					return url.href + "/api";
				}
			}
		} catch (e) {
			console.log(e.message);
		}
	}
	return null;
};

module.exports.createDHIS2Auth = () => {
	const username = process.env.DHIS2_USER;
	const password = process.env.DHIS2_PASS;
	return {username, password};
};
module.exports.createDHIS2Auth2 = () => {
	const username = process.env.DHIS2_USER_HMIS;
	const password = process.env.DHIS2_PASS_HMIS;
	return {username, password};
};

module.exports.getDHIS2Url = () => {
	const uri = process.env.DHIS2_URL;
	return this.getDHIS2Url1(uri);
};

module.exports.getDHIS2Url2 = () => {
	const uri = process.env.DHIS2_URL_HMIS;
	return this.getDHIS2Url1(uri);
};

module.exports.queryDHIS2 = async (path, params) => {
	try {
		const baseUrl = this.getDHIS2Url();
		if (baseUrl) {
			const urlx = `${baseUrl}/${path}`;
			const {data} = await axios.get(urlx, {
				auth: this.createDHIS2Auth(),
				params,
			});
			return data;
		}
	} catch (e) {
		console.log(e.message);
	}
};

module.exports.query2DHIS2 = async (path, params) => {
	try {
		const baseUrl = this.getDHIS2Url2();
		if (baseUrl) {
			const urlx = `${baseUrl}/${path}`;
			const {data} = await axios.get(urlx, {
				auth: this.createDHIS2Auth2(),
				params,
			});
			return data;
		}
	} catch (e) {
		console.log(e.message);
	}
};

module.exports.downloadData = async (
	remoteDataSet,
	orgUnit,
	period
) => {
	const baseUrl = this.getDHIS2Url2();
	let remoteUrl = `${baseUrl}/dataValueSets.csv`;
	const response = await Axios({
		url: remoteUrl,
		method: "GET",
		responseType: "stream",
		auth: this.createDHIS2Auth2(),
		params: {
			dataSet: remoteDataSet,
			period,
			orgUnit,
			children: true,
		},
	});
	response.data.pipe(fs.createWriteStream(`${remoteDataSet}.csv`));
	return new Promise((resolve, reject) => {
		response.data.on("end", () => {
			resolve();
		});
		response.data.on("error", () => {
			reject();
		});
	});
}

module.exports.processFile = async (fileName) => {
	const dataValues = [];
	return new Promise((resolve, reject) => {
		const stream = fs.createReadStream(`${fileName}.csv`);
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
					orgunit: orgUnit,
					categoryoptioncombo: categoryOptionCombo,
					attributeoptioncombo: attributeOptionCombo,
					value,
					storedby: storedBy,
					lastupdated: lastUpdatedBy,
					comment,
					followup
				} = row
				dataValues.push({
					dataElement,
					period,
					orgUnit,
					categoryOptionCombo,
					attributeOptionCombo,
					value,
					storedBy,
					lastUpdatedBy,
					comment,
					followup
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

module.exports.postDHIS2 = async (path, postData, params) => {
	try {
		const baseUrl = this.getDHIS2Url();
		if (baseUrl) {
			const urlx = `${baseUrl}/${path}`;
			const {data} = await axios.post(urlx, postData, {
				auth: this.createDHIS2Auth(),
				params,
			});
			return data;
		}
	} catch (e) {
		console.log(e.message);
	}
};

module.exports.post2DHIS2 = async (path, postData, params) => {
	try {
		const baseUrl = this.getDHIS2Url2();
		if (baseUrl) {
			const urlx = `${baseUrl}/${path}`;
			const {data} = await axios.post(urlx, postData, {
				auth: this.createDHIS2Auth2(),
				params,
			});
			return data;
		}
	} catch (e) {
		console.log(e.message);
	}
};

module.exports.deleteDHIS2 = async (path) => {
	try {
		const baseUrl = getDHIS2Url();
		if (baseUrl) {
			const urlx = `${baseUrl}/${path}`;
			const {data} = await axios.delete(urlx, {
				auth: this.createDHIS2Auth(),
			});
			return data;
		}
	} catch (e) {
		console.log(e.message);
	}
};

module.exports.updateDHIS2 = async (path, postData, params) => {
	try {
		const baseUrl = getDHIS2Url();
		if (baseUrl) {
			const urlx = `${baseUrl}/${path}`;
			const {data} = await axios.put(urlx, postData, {
				auth: this.createDHIS2Auth(),
				params,
			});
			return data;
		}
	} catch (e) {
		console.log(e.message);
	}
};

module.exports.uploadDHIS2 = async (path, file, fileName, params) => {
	try {
		const baseUrl = getDHIS2Url();
		if (baseUrl) {
			const urlx = `${baseUrl}/${path}`;
			const form = new FormData();
			form.append(fileName, file, `${fileName}.csv`);
			const {data} = await axios.post(urlx, form, {
				auth: this.createDHIS2Auth(),
				params,
				headers: form.getHeaders(),
			});
			return data;
		}
	} catch (e) {
		console.log(e.message);
	}
};

module.exports.validText = (dataType, value) => {
	switch (dataType) {
		case "TEXT":
		case "LONG_TEXT":
			return !!value;
		case "NUMBER":
			return !isNaN(Number(value));
		case "EMAIL":
			const re = /\S+@\S+\.\S+/;
			return re.test(String(value).toLowerCase());
		case "BOOLEAN":
			return (
				String(value).toLowerCase() === "false" ||
				String(value).toLowerCase() === "true"
			);
		case "TRUE_ONLY":
			return String(value).toLowerCase() === "true";
		case "PERCENTAGE":
			return Number(value) >= 0 && Number(value) <= 100;
		case "INTEGER":
			return !isNaN(Number(value)) && Number.isInteger(Number(value));
		case "UNIT_INTERVAL":
			return Number(value) >= 0 && Number(value) <= 1;
		case "INTEGER_NEGATIVE":
		case "NEGATIVE_INTEGER":
			return Number.isInteger(Number(value)) && Number(value) < 0;
		case "INTEGER_ZERO_OR_POSITIVE":
		case "AGE":
			const v = Number(value);
			return !isNaN(v) && Number.isInteger(v) && v >= 0;
		case "COORDINATE":
			try {
				const c = JSON.parse(value);
				return _.isArray(c) && c.length === 2;
			} catch (e) {
				return false;
			}
		default:
			return true;
	}
};

module.exports.validateValue = (dataType, value, optionSetValue, optionSet) => {
	if (optionSetValue && !!value) {
		const options = optionSet.options.map((o) => {
			return {
				code: o.code,
				value: o.value,
			};
		});
		const coded = options.find((o) => {
			return (
				String(value).toLowerCase() === String(o.code).toLowerCase() ||
				String(value).toLowerCase() === String(o.value).toLowerCase()
			);
		});
		if (!!coded) {
			return coded.code;
		}
	} else if (!!value && this.validText(dataType, value)) {
		if ((dataType === "BOOLEAN") | (dataType === "TRUE_ONLY")) {
			return String(value).toLowerCase();
		}
		return value;
	}
	return null;
};

module.exports.readCSV = (fileName) => {
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

module.exports.sendMail = () => {
	const transporter = nodemailer.createTransport({
		host: "smtp.gmail.com",
		port: 587,
		auth: {
			user: process.env.GMAIL_USERNAME,
			pass: process.env.GMAIL_PASSWORD,
		},
	});

	transporter.verify().then(console.log).catch(console.error);
};

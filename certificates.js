const { epivac } = require("./epivac");
const { createIndex } = require("./elasticsearch");
const args = process.argv.slice(2);

try {
  createIndex({ index: "certificates" });
} catch (error) {
  console.log(error.message)
}

if (args.length === 3) {
  epivac(args[0], args[1], args[2]);
} else {
  console.log("Wrong arguments");
}

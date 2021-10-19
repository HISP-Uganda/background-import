const { syncFacilities } = require("./epivac");
const args = process.argv.slice(2);

if (args.length === 3) {
  syncFacilities(args[0], args[1], args[2]);
} else {
  console.log("Wrong arguments");
}

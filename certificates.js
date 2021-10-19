const { epivac } = require("./epivac");
const args = process.argv.slice(2);
if (args.length === 4) {
  epivac(args[0], args[1], args[2], args[3]);
} else {
  console.log("Wrong arguments");
}

const { createLogger, format, transports } = require("winston");
require("winston-daily-rotate-file");

const logDir = "logs";

const dailyRotateFileTransport = (filename) =>
  new transports.DailyRotateFile({
    filename: `${logDir}/%DATE%-${filename}.log`,
    maxSize: "1g",
    maxDays: "3d",
    zippedArchive: true,
    datePattern: "YYYY-MM-DD",
  });

const logger = function (filename) {
  return createLogger({
    level: "debug",
    maxsize: "500m",
    format: format.combine(
      format.timestamp({
        format: "YYYY-MM-DD HH:mm:ss",
      }),
      // for the log file
      format.printf(
        (info) => `${info.timestamp} ${info.level}: ${info.message}`
      )
    ),
    transports: [
      new transports.Console({
        level: "info",
        format: format.combine(
          format.colorize(),
          format.printf((info) => `${info.message}`)
        ),
      }),
      dailyRotateFileTransport(filename),
    ],
  });
};

module.exports = logger;

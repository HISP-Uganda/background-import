const { Client } = require("@elastic/elasticsearch");
const client = new Client({ node: "http://localhost:9200" });

const logger = require("./Logger");

const log = logger("es");

module.exports.createIndex = async (params) => {
  let body = {
    index: params.index,
  };
  if (params.body) {
    body = { ...body, body: params.body };
  }
  try {
    return await client.indices.create(body);
  } catch (error) {
    log.error(error.message);
  }
};

module.exports.search = async (params) => {
  const { index, body } = params;
  try {
    const {
      body: {
        hits: { hits },
      },
    } = await client.search({
      index,
      body,
    });
    return hits;
  } catch (error) {
    log.error(error.message);
  }
};

module.exports.bulk = async (params) => {
  const { index, dataset, id } = params;
  const body = dataset.flatMap((doc) => [
    { index: { _index: index, _id: doc[id] } },
    doc,
  ]);
  try {
    const { body: bulkResponse } = await client.bulk({
      refresh: true,
      body,
    });
    const errorDocuments = [];
    if (bulkResponse.errors) {
      bulkResponse.items.forEach((action, i) => {
        const operation = Object.keys(action)[0];
        if (action[operation].error) {
          errorDocuments.push({
            status: action[operation].status,
            error: action[operation].error,
            operation: body[i * 2],
            document: body[i * 2 + 1],
          });
        }
      });
    }
    log.info(`Inserted ${dataset.length - errorDocuments.length}`);
    return {
      errorDocuments,
      inserted: dataset.length - errorDocuments.length,
    };
  } catch (error) {
    log.error(error.message);
  }
};

module.exports.searchByIdAndPhone = async (params) => {
  const { phone, identifier, index } = params;
  try {
    const {
      body: {
        hits: { hits },
      },
    } = await client.search({
      index,
      body: {
        query: {
          bool: {
            must: [{ match: { id: identifier } }],
          },
        },
      },
    });
    if (hits.length > 0) {
      return hits[0]._source;
    }
  } catch (error) {
    log.error(error.message);
  }
};

module.exports.searchByValues = async (params) => {
  const { term, values, index } = params;
  try {
    const {
      body: {
        hits: { hits },
      },
    } = await client.search({
      index,
      body: {
        query: {
          bool: {
            filter: {
              terms: { [`${term}.keyword`]: values },
            },
          },
        },
      },
    });
    if (hits.length > 0) {
      return hits[0]._source;
    }
  } catch (error) {
    log.error(error.message);
  }
};

module.exports.searchByTrackedEntityInstance = async (params) => {
  const { trackedEntityInstance, index } = params;
  try {
    const {
      body: {
        hits: { hits },
      },
    } = await client.search({
      index,
      body: {
        query: {
          match: { trackedEntityInstance },
        },
      },
    });
    if (hits.length > 0) {
      return hits[0]._source;
    }
  } catch (error) {
    log.error(error.message);
  }
};

module.exports.get = async (params) => {
  try {
    const { index, id } = params;
    const {
      body: { _source },
    } = await client.get({
      index,
      id,
    });
    return _source;
  } catch (error) {
    log.error(error.message);
  }
};

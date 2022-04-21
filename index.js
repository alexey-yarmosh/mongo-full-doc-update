const { MongoClient } = require('mongodb');
const _ = require('lodash');

const url = 'mongodb://admin:admin@localhost:27017';
const client = new MongoClient(url);
const dbName = 'test';

async function clear() {
  const db = client.db(dbName);
  const collection = db.collection('mongo-fu');
  await collection.drop();
}

async function createDocument() {
  const db = client.db(dbName);
  const collection = db.collection('mongo-fu');
  await collection.insertOne({
    productId: 1,
    version: 1,
    productName: 'product 1',
    options: [{
      optionId: 1,
      optionName: 'option 1',
      skus: [{
        skuId: 1,
        skuName: 'sku 1',
        colors: [{
          colorId: 1,
          colorName: 'color 1',
          date: new Date('01-01-2022')
        }, {
          colorId: 2,
          colorName: 'color 2',
          date: new Date('02-01-2022')
        }]
      }, {
        skuId: 2,
        skuName: 'sku 2',
        colors: []
      }]
    }, {
      optionId: 2,
      optionName: 'option 2',
      skus: []
    }]
  })

  return 'done.';
}

async function logDocument() {
  const db = client.db(dbName);
  const collection = db.collection('mongo-fu');
  const productId = 1;

  const document = await collection.findOne({ productId });
  console.dir(document, { depth: null });
}

async function updateDocument() {
  const db = client.db(dbName);
  const collection = db.collection('mongo-fu');
  const productId = 1;
  const optionId = 1;
  const skuId = 1;
  const colorId = 1;
  const colorName = 'color 3';
  const date = new Date('02-03-2022');

  const colorExistsFilter = {
    productId,
    options: { $elemMatch: { optionId, skus: { $elemMatch: { skuId, colors: { $elemMatch: { colorId, date: { $lte: date } } } } } } }
  };
  const updateColor = {
    $set: {
      "options.$[option].skus.$[sku].colors.$[color]": {
        colorId,
        colorName,
        date
      }
    },
  };
  const colorUpdateArrayFilters = [
    {
      "option.optionId": optionId,
    },
    {
      "sku.skuId": skuId
    },
    {
      "color.colorId": colorId
    }
  ];

  const colorNotExistsFilter = {
    productId,
    options: { $elemMatch: { optionId, skus: { $elemMatch: { skuId, colors: { $not: { $elemMatch: { colorId } } } } } } }
  };
  const pushColor = {
    $push: {
      "options.$[option].skus.$[sku].colors": {
        colorId,
        colorName,
        date
      }
    },
  };
  const colorPushArrayFilters = [
    {
      "option.optionId": optionId,
    },
    {
      "sku.skuId": skuId
    }
  ];

  const result = await collection.bulkWrite([
    {
      updateOne: {
        filter: colorExistsFilter,
        update: updateColor,
        arrayFilters: colorUpdateArrayFilters
      }
    },
    {
      updateOne: {
        filter: colorNotExistsFilter,
        update: pushColor,
        arrayFilters: colorPushArrayFilters
      }
    }
  ], { ordered: true });

  return 'done.';
}

async function updateDocument2() {
  const db = client.db(dbName);
  const collection = db.collection('mongo-fu');
  const productId = 1;
  const optionId = 1;
  const skuId = 1;
  const colorId = 1;
  const colorName = 'color 3';
  const date = new Date('02-03-2022');

  const result = await collection.updateOne({
    productId,
    options: { $elemMatch: { optionId } }
  }, [{
    $set: {
      options: {
        $map: {
          input: '$options',
          as: 'option',
          in: {
            $cond: {
              if: { $eq: ['$$option.optionId', optionId] },
              then: {
                $mergeObjects: ['$$option', {
                  skus: {
                    $map: {
                      input: '$$option.skus',
                      as: 'sku',
                      in: {
                        $cond: {
                          if: { $eq: ['$$sku.skuId', skuId] },
                          then: {
                            $mergeObjects: ['$$sku', {
                              colors: {
                                $cond: {
                                  if: { $in: [colorId, '$$sku.colors.colorId'] },
                                  then: {
                                    $map: {
                                      input: '$$sku.colors',
                                      as: 'color',
                                      in: {
                                        $cond: {
                                          if: { $and: [{ $eq: ['$$color.colorId', colorId] }, { $lte: ['$$color.date', date] }] },
                                          then: { $mergeObjects: ['$$color', { colorId, colorName, date }] },
                                          else: '$$color'
                                        },
                                      }
                                    }
                                  },
                                  else: { $concatArrays: ['$$sku.colors', [{ colorId, colorName, date }]] }
                                }
                              }
                            }]
                          },
                          else: '$$sku'
                        }
                      }
                    }
                  }
                }]
              },
              else: '$$option'
            }
          }
        }
      }
    },
  }]);

  console.log('result', result);

  return 'done.';
}

async function updateDocument3() {
  const db = client.db(dbName);
  const collection = db.collection('mongo-fu');
  const productId = 1;
  const optionId = 1;
  const skuId = 1;
  const colorId = 1;
  const colorName = 'color 3';
  const date = new Date('02-03-2022');

  const document = await collection.findOne({
    productId,
    options: { $elemMatch: { optionId, skus: { $elemMatch: { skuId } } } }
  });

  const option = _.find(document.options, { optionId });
  const sku = _.find(option.skus, { skuId });
  const colors = sku.colors;
  const color = _.find(colors, { colorId });

  if (!color) {
    colors.push({ colorId, colorName, date });
  } else if (color.date < date) {
    color.colorId = colorId;
    color.colorName = colorName;
    color.date = date;
  }

  const result = await replaceOne(updateDocument3, {
    _id: document._id
  }, document);

  console.log('result', result);

  return 'done.';
}

async function replaceOne(updateDocument3, filter, replacement, options, callback) {
  const db = client.db(dbName);
  const collection = db.collection('mongo-fu');

  const currentVersion = replacement.version;
  const newVersion = currentVersion + 1;
  const result = await collection.replaceOne({ ...filter, version: currentVersion }, { ...replacement, version: newVersion }, options, callback);
  console.log('do while result', result);
  if (result.matchedCount === 0) { // if a document is not found fo any other reason than the version (e.g. deletion) the cycle is infinite
    await updateDocument3();
  }

  return result;
}

(async () => {
  try {
    await client.connect();
    console.log('Connected successfully to server');
    await clear();
    await createDocument();
    await logDocument();
    // if no such color => add, if date is fresher => update, either ignore. (3 implementations)
    // const result = await updateDocument();
    // const result = await updateDocument2();
    const result = await updateDocument3();
    await logDocument();
    console.log('result', result);
  } catch (err) {
    console.error(err);
  }
  await client.close();
})();

const Datastore = require('@google-cloud/datastore');
const {Storage} = require('@google-cloud/storage');
const vision = require('@google-cloud/vision');

const datastore = Datastore();
const storage = new Storage();
const client = new vision.ImageAnnotatorClient();

exports.saveTags = (event) => {
  return getKey(event.data).then(key => {
    return processLabels(getStoragePath(event.data), key).then(() => {
    console.log('Successfully inserted labels');
  })
  .catch(err => {
    console.error('Vision API failed', err);
    });
  });
}

exports.deleteTagger = (event) => {
  return getKey(event.data).then(key => {
    return datastore.delete(key).then(() => {
      console.log('Successfully deleted entity.', key);
    })
    .catch(err => {
      console.error('Vision API failed:', err);
    });
  });
}

function getKey(bucketObject) {
  if(!bucketObject.contentType.startsWith('image/')) {
    return Promise.reject("object is not an image");
  }

  const storagePath = getStoragePath(bucketObject);
  const query = datastore.createQuery('Images').select('__key__').limit(1);
  query.filter('storagePath', '=', storagePath);

  return query.run().then(data => {
    const objectExists = data[0].length > 0;
    const key = objectExists ? data[0][0][datastore.KEY] : datastore.key('Images');
    return key;
  })
  .catch(err => {
    console.error("Could not get the targeted bucket:", err);
  });
}

function processLabels(storagePath, key) {
  return client.labelDetection(storagePath).then(results => {
    const labels = results[0].labelAnnotations;
    console.log("labels", labels);
    const descriptions = labels.filter(label => label.score >= 0.65)
          .map(label => label.description);
    console.info("descriptions", descriptions);
    return datastore.save({
      key: key,
      data: {
        storagePath: storagePath,
        tags: descriptions
      }
    });
  })
}

function getStoragePath(bucketObject) {
  return `gs://${bucketObject.bucket}/${bucketObject.name}`;
}


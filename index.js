const exec = require('child_process');
const path = require('path');
const os = require('os');
const fs = require('fs')

const Datastore = require('@google-cloud/datastore');
const {Storage} = require('@google-cloud/storage');
const vision = require('@google-cloud/vision');


const datastore = Datastore();
const storage = new Storage();
const client = new vision.ImageAnnotatorClient();
const tmpPath = os.tmpdir();

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
    return {
      key: key,
      data: {
        storagePath: storagePath,
        tags: descriptions
      }
    };
  })
}

function getStoragePath(bucketObject) {
  return `gs://${bucketObject.bucket}/${bucketObject.name}`;
}

function generateThumbnail(bucketObject) {
  const filePath = bucketObject.name;
  const parsedPath = path.parse(bucketObject.name);
  const fileName = parsedPath.base;

  const bucket = storage.bucket(bucketObject.bucket);
  const file = bucket.file(bucketObject.name);

  const tempLocalDir = path.join(tmpPath, parsedPath.dir);
  const tempLocalFile = path.join(tmpLocalDir, fileName);

  return mkDirAsync(tempLocalDir);
}

function mkDirAsync(dir) {
  return new Promise((resolve, reject) => {
    fs.lstat(dir, (err, reject) => {
      if (err) {
        if (err.code === 'ENOENT') {
          fs.mkdir(dir, (err) => {
            if (err) {
              reject(err);
            } else {
              console.log('created directory');
              resolve();
            }
          });
        } else {
          reject(err);
        }
      } else {
        if (stats.isDirectory()) {
          console.log(`${dir} already exists!`);
        } else {
          reject(new Error('A directory was not passed to this function!'));
        }
      }
    })
  });
}


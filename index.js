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
  const data = event.data
  return getKey(data).then(key => {
    if (path.parse(data.name).dir !== 'uploads') {
      console.log('Only processing images from the upload folder');
      return Promise.resolve();
    }

    const labels = processLabels(getStoragePath(data), key);
    const thumbnail = generateThumbnail(data);

    return Promise.all([thumbnail, labels]).then(results => {
      console.log(results);
      const entity = results[1];

      const thumbnailName = results[0][0].name;
      const thumbnailPath = `gs://${data.bucket}/${thumbnailName}`;
      entity.data.thumbnailPath = thumbnailPath;
      return datastore.save(entity);
    });

    console.log('Successfully inserted labels');
  })
  .catch(err => {
    console.error('Vision API failed', err);
  });
}

exports.deleteTagger = (event) => {
  return getKey(event.data).then(key => {
    if (!key) {
      return Promise.resolve();
    }

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
    const key = objectExists ? data[0][0][datastore.KEY] : null;
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

  return mkDirAsync(tempLocalDir)
    .then(() => {
      return file.download({ destination: tempLocalFile});
    })
    .catch(err => {
      console.error('Failed to download file.', file);
      return Promise.reject(err);
    })
    .then(() => {
      console.log(`${file.name} successfully downloaded to ${tempLocalFile}`);

      return new Promise((resolve, reject) => {
        const escapedFile = tempLocalFile.replace(/(\s+)/g, '\\$1');
        //ImageMagick is available on the vision api
        exec(`convert ${escapedFile} -thumbnail '200x200' ${escapedFile}`, (err, stdout) => {
          if (err) {
            console.error('Failed to resize image', err);
            reject();
          } else {
            resolve(stdout);
          }
        });
      })
    })
    .then(() => {
      console.log(`Image ${fileName} successfully resized to 200x200`);
      const thumbnailFileName = path.join('thumbnails', fileName);

      return bucket.upload(tempLocalFile, {destination: thumbnailFileName})
        .catch(err => {
          console.error('Failed to upload resized image', err);
          return Promise.reject(err);
        });
    })
    .then((newFileObject) => {
      return new Promise((resolve, reject) => {
        console.log('Unlinking file');
        fs.unlink(tempLocalFile, err => {
          if (err) {
            reject(err);
          } else {
            resolve(newFileObject);
          }
        });
      });
    });
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


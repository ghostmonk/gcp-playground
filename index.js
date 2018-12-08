const performance = require('perf_hooks').performance;
const exec = require('child_process').exec;
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
const t0 = performance.now();

exports.saveTags = (event) => {
  console.log("saveTags-EVENT:", event, perf());
  return getKey(event).then(key => {
    if (path.parse(event.name).dir !== 'uploads') {
      console.log('Only processing images from the upload folder', perf());
      return Promise.resolve();
    }
    console.log("saveTags-KEY:", key, perf());
    return saveEntity(event, key);
  })
  .catch(err => {
    console.error('Vision API failed', err, perf());
  });
}

exports.deleteTagger = (event) => {
  console.log("deleteTagger:EVENT:", event, perf());
  return getKey(event).then(key => {
    if (!key) {
      return Promise.resolve();
    }

    return datastore.delete(key).then(() => {
      console.log('Successfully deleted entity.', key, perf());
    })
    .catch(err => {
      console.error('Vision API failed:', err, perf());
    });
  });
}

function saveEntity(data, key) {
  const labels = processLabels(getStoragePath(data), key);
  console.log("saveEntity:LABELS:", labels, perf());
  const thumbnail = generateThumbnail(data);
  console.log("saveEntity:THUMBNAIL:", thumbnail, perf());
  return Promise.all([thumbnail, labels]).then(results => {
    const entity = results[1];
    const thumbnailName = results[0][0].name;
    const thumbnailPath = `gs://${data.bucket}/${thumbnailName}`;
    entity.data.thumbnailPath = thumbnailPath;
    console.log("Saving Entity -->", entity, perf());
    return datastore.save(entity);
  });
}

function getKey(bucketObject) {
  console.log("getKey:BUCKET OBJET:", bucketObject, perf());
  if(!bucketObject.contentType.startsWith('image/')) {
    return Promise.reject("object is not an image");
  }

  const storagePath = getStoragePath(bucketObject);
  console.log("getKey:STORAGEPATH:", storagePath, perf());
  const query = datastore.createQuery('Images').select('__key__').limit(1);
  console.log("getKey:QUERY:", query, perf());
  query.filter('storagePath', '=', storagePath);

  return query.run().then(data => {
    console.log("storagePath:QUERYRUN:", data, perf());
    const objectExists = data[0].length > 0;
    const key = objectExists ? data[0][0][datastore.KEY] : datastore.key('Images');
    return key;
  })
  .catch(err => {
    console.error("Could not get the targeted bucket:", err, perf());
  });
}

function processLabels(storagePath, key) {
  console.log("processLabels:STOREAGEPATH:", storagePath, perf());
  return client.labelDetection(storagePath).then(results => {
    console.log("processLabels:LABELDETECTIONRESULTS:", results, perf());
    const labels = results[0].labelAnnotations;
    console.log("labels", labels, perf());
    const descriptions = labels.filter(label => label.score >= 0.65)
          .map(label => label.description);
    console.info("descriptions", descriptions, perf());
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
  console.log("FILEPATH:", filePath, perf());
  const parsedPath = path.parse(filePath);
  console.log("PARSEDPATH:", parsedPath, perf());
  const fileName = parsedPath.base;
  console.log("FILENAME:", fileName, perf());

  const bucket = storage.bucket(bucketObject.bucket);
  console.log("BUCKET:", bucket, perf());
  const file = bucket.file(filePath);
  console.log("FILE", file, perf());

  const tempLocalDir = path.join(tmpPath, parsedPath.dir);
  console.log("TEMPLOCALDIR", tempLocalDir, perf());
  const tempLocalFile = path.join(tempLocalDir, fileName);
  console.log("TEMPLOCALFILE", tempLocalFile, perf());

  return mkDirAsync(tempLocalDir)
    .then(() => {
      return file.download({ destination: tempLocalFile});
    })
    .catch(err => {
      console.error('Failed to download file.', file, perf());
      return Promise.reject(err);
    })
    .then(() => {
      console.log(`${file.name} successfully downloaded to ${tempLocalFile}`, perf());
      return createThumb(tempLocalFile);
   })
   .then(() => {
     console.log(`Image ${fileName} successfully resized to 200x200`, perf());
     return uploadThumbnail(bucket, fileName, tempLocalFile);
   })
   .then((newFileObject) => {
     console.log("UNLINKING FILE: NEW FILE OBJECT:", newFileObject, perf());
     return unlinkFile(newFileObject, tempLocalFile);
   });
}

function unlinkFile(newFile, tempFile) {
  return new Promise((resolve, reject) => {
    console.log('Unlinking file', perf());
    fs.unlink(tempFile, err => {
      if (err) {
        reject(err);
      } else {
        resolve(newFile);
      }
    });
  });
}

function uploadThumbnail(bucket, fileName, tempPath) {
  const thumbnailFileName = path.join('thumbnails', fileName);
  console.log("******THUMBS*******", thumbnailFileName, perf());
  return bucket.upload(tempPath, {destination: thumbnailFileName})
    .catch(err => {
      console.error('Failed to upload resized image', err, perf());
      return Promise.reject(err);
    });
}

function createThumb(originalImagePath) {
  console.log("createThumb:START:", originalImagePath, perf());
  return new Promise((resolve, reject) => {
    const escapedFile = originalImagePath.replace(/(\s+)/g, '\\$1');
    //ImageMagick is available on the vision api
    console.log("starting exec:ESCAPEDFILE:", escapedFile, perf());
    exec(`convert ${escapedFile} -thumbnail '200x200' ${escapedFile}`, (err, stdout) => {
      console.log("convert complete:MSG", stdout, perf());
      if (err) {
        console.error('Failed to resize image', err);
        reject();
      } else {
        resolve(stdout);
      }
    });
  });
}

function mkDirAsync(dir) {
  return new Promise((resolve, reject) => {
    fs.lstat(dir, (err, stats) => {
      if (err) {
        console.log("MKDIR ERROR:", err, perf()); 
        if (err.code === 'ENOENT') {
          fs.mkdir(dir, (err) => {
            if (err) {
              reject(err);
            } else {
              console.log('created directory', perf());
              resolve();
            }
          });
        } else {
          reject(err);
        }
      } else {
        if (stats.isDirectory()) {
          console.log(`${dir} already exists!`, perf());
        } else {
          reject(new Error('A directory was not passed to this function!'));
        }
      }
    })
  });
}

function perf() {
  return performance.now() - t0;
}

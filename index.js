var express = require('express');
var bodyParser = require('body-parser');
var async = require('async');
var app = express();
var fs = require('fs');
var archiver = require('archiver');
var unzip = require('unzip2');
const path = require('path');
// Use the built-in express middleware for serving static files from './public'
app.use(express.static('public'));

var portNum = (process.env.PORT || 5000);
var server = app.listen(portNum, function(){
    console.log("Node app is running : " + server.address().port);
});

// Imports the Google Cloud client library.
const {Storage} = require('@google-cloud/storage');
const os = require('os');
const tmp = os.tmpdir();
//console.log(options)
function writeValToFile() {

//    tmp = os.tmpdir()
    const destFileName = '/timestamp1';
    const dirPath = path.join(tmp, destFileName);
    const options = { destination: dirPath, };
    let writeStream = fs.createWriteStream(dirPath);
    console.log(options)
    writeStream.write('asdasdasd');
    writeStream.end();
 }
writeValToFile();
// Instantiates a client. If you don't specify credentials when constructing
// the client, the client library will look for credentials in the
// environment.
//const storage = new Storage();
// Makes an authenticated API request.
async function listBuckets() {
  try {
    const results = await storage.getBuckets();

    const [buckets] = results;

    console.log('Buckets:');
    buckets.forEach(bucket => {
      console.log(bucket.name);
    });
  } catch (err) {
    console.error('ERROR:', err);
  }
}
//listBuckets();
//const annotaionsbucketFiles = storage.bucket('annotations-bucket');
app.use(express.static(__dirname + '/public'));
app.use(bodyParser.json({limit: '700mb'}));
app.use(bodyParser.urlencoded({limit: '700mb', extended: true, parameterLimit:50000}));
app.post('/save', function(req, res){
	try {
		async.waterfall([
			function(callback){
//				if (!(fs.existsSync('./public/spool'))){
//					fs.mkdirSync('./public/spool');
//				}
				var dat = new Buffer(req.body.b64, 'base64');
				var fileName = req.body.fileName;
				var filePath = path.join(tmp,'/public/spool/') + fileName;
//				console.log('filepath', filePath);

				fs.writeFile(filePath, dat, function(err){
                    upload_to_cloud(filePath,fileName, 'public').then(function(results){
                        callback(null, fileName);
                    });
				});
			}], function (err, result){
				if(err){
					console.log('Error! ' + result);
				} else {
					res.send(result);
				}
			});
	} catch(err) {
		myError(err, res);
	}
});
app.post('/save_zip', function(req, res){
		var retObj = {};
		async.waterfall([
			function(callback){
				var dat = new Buffer(req.body.b64, 'base64');
				var fileName = req.body.fileName;
//				var filePath = './data/zip/' + fileName;
				var filePath = path.join(tmp,'/data/zip/') + fileName;
				fs.writeFile(filePath, dat, function(err){
					callback(null, filePath);
				});
				upload_to_cloud(filePath,fileName, 'public').then(function(results){
                        console.log("zip file uploaded")});
			},

			function(zipFilePath, callback){
				fs.createReadStream(zipFilePath)
				  .pipe(unzip.Parse())
				  .on('entry', function (entry) {
				    var i = entry.path;
				    var type = entry.type; // 'Directory' or 'File'
				    var size = entry.size;
				    if (i.match(/^examples\/.+/) && type == 'File'){
				    	var fileName = i.replace(/^examples\//g, '');
						var filePath = path.join(tmp,'/public/spool/') + fileName;
						entry.pipe(fs.createWriteStream(filePath));
				    } else if (i == 'annotations.csv'){
				    	var csvData = '';
				    	entry.on('data', function (dat){
				    		csvData += dat;
						});
				    	entry.on('end', function (){
				    		var dat = csvData;
				    		var csvObj = {};
    						var fileList = [];
							var acsv = dat.toString();
							var recs = acsv.split(/\r\n|\r|\n/);
							for (var i = 1; i < recs.length; i++){
								var rec = recs[i].replace(/""/g, '"').replace(/"\{/g, '{').replace(/\}"/g, '}');
								var splits = rec.split(/,/);
								var thisName = splits[0].replace(/"/g, '');
								splits.shift();
								var rec2 = '[' + splits.join(',') + ']';
								if (thisName && thisName.length > 0){
									csvObj[thisName] = JSON.parse(rec2);
									fileList.push(thisName);
								}
								retObj = {csv: csvObj, files: fileList};
							}
				    	});
				    } else {
				      	entry.autodrain();
				    }
				  }).on('close', function(){
						callback(null, retObj);			  	
				  });
			}
		], function (err, result){
			if(err){
				console.log('Error! ' + result);
			} else {
				res.send(result);
			}
		});
});
app.post('/csv2zip', function(req, res){
	try {
		async.waterfall([
			function(callback){
				var csvObj = JSON.parse(req.body.csv);
				createZipFile(csvObj, callback);
			}], function (err, result){
				if(err){
					console.log('Error! ' + result);
				} else {
					res.send(result);
				}
			});
	} catch(err) {
		myError(err, res);
	}
});
module.exports = app;
initDirectory();


function myError(err, res){
    res.send(err);
}

function createZipFile(csvObj, callback){
	var fileName = 'train_' + getRandomStr() + '.zip';
	var filePath = path.join(tmp,'/data/') + fileName;
	var output = fs.createWriteStream(filePath);
	output.on('close', function() {
		callback(null, {
			name : fileName,
			url : 'https://storage.googleapis.com/test-annotations-bucket' + '/zip/' + fileName
		});
	});
	var archive = archiver('zip', {
    	zlib: { level: 9 }
	});
	archive.pipe(output);
	archive.append(csvObj.csv, { name: 'annotations.csv' });
	var data = csvObj.file;
	for (var key in data){
		if (data.hasOwnProperty(key)){
			archive.append(fs.createReadStream(path.join(tmp,'/public/spool/') + key), { name: 'examples/' + key });
		}
	}
	archive.finalize();
	upload_to_cloud(filePath,fileName, 'zip').then(function(results){
                        console.log("zip file uploaded")});
}
function upload_to_cloud(filePath,fileName, folder){

    var faker = require('faker');
    const path = require('path');

//    const myStorage = new Storage({keyFilename: 'C:/Users/safiu/Downloads/gc-function-models-79c8416331e0.json', projectId:'gc-function-models'});
    const myStorage = new Storage();
    const bucket = myStorage.bucket('test-annotations-bucket');
    //const filePath = path.resolve(__dirname, `${fileName}`);
//    const uuid = 'public';

    res = bucket.upload(filePath, {
      destination: `${folder}/${fileName}`,
      gzip: true,
      metadata: {
        cacheControl: 'public, max-age=31536000'
      }
    });
    console.log(fileName, "uploaded successfully at ", Date.now());
    return res;
}
//save_file('test/gc-function-models-ca21fd537c42.json');
function initDirectory(){
//    tmp = os.tmpdir();
    if (!fs.existsSync(path.join(tmp,'/public'))){
        fs.mkdirSync(path.join(tmp,'/public'));
    }
    if (!fs.existsSync(path.join(tmp,'/public/spool'))){
        fs.mkdirSync(path.join(tmp,'/public/spool'));
    }
	if (!(fs.existsSync(path.join(tmp,'/public/spool')) && fs.statSync(path.join(tmp,'/public/spool')).isDirectory()))
	{
	    fs.mkdirSync(path.join(tmp,'/public/spool'));
	}

	if (!fs.existsSync(path.join(tmp,'/public/data'))){
        fs.mkdirSync(path.join(tmp,'/public/data'));
    }
	if (!(fs.existsSync(path.join(tmp,'/public/data')) && fs.statSync(path.join(tmp,'/public/spool')).isDirectory()))
	{
	    fs.mkdirSync(path.join(tmp,'/public/data'));
	}

    if (!fs.existsSync(path.join(tmp,'/data'))){
        fs.mkdirSync(path.join(tmp,'/data'));
    }
	if (!(fs.existsSync(path.join(tmp,'/data')) && fs.statSync(path.join(tmp,'/data')).isDirectory()))
	{
	    fs.mkdirSync(path.join(tmp,'/data'));
	}

	if (!fs.existsSync(path.join(tmp,'/data/zip'))){
        fs.mkdirSync(path.join(tmp,'/data/zip'));
    }
	if (!(fs.existsSync(path.join(tmp,'/data/zip')) && fs.statSync(path.join(tmp,'/data/zip')).isDirectory()))
	{
	    fs.mkdirSync(path.join(tmp,'/data/zip'));
	}

//	if (!(fs.existsSync('./public/spool') && fs.statSync('./public/spool').isDirectory())) fs.mkdirSync('./public/spool')
//	if (!(fs.existsSync('./public/data') && fs.statSync('./public/data').isDirectory())) fs.mkdirSync('./public/data');
//	if (!(fs.existsSync('./data') && fs.statSync('./data').isDirectory())) fs.mkdirSync('./data');
//	if (!(fs.existsSync('./data/zip') && fs.statSync('./data/zip').isDirectory())) fs.mkdirSync('./data/zip');
	console.log('Init Folders');  
}

function getRandomStr(){
	return Math.floor(100000000000000000 * Math.random()).toString(32);
}


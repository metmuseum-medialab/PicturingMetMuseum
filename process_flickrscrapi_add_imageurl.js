/*
processing csv file, running through scrapi to get additional data
*/

var scrapiSearch = "http://scrapi.org/search/{term}";

var scrapiObject = "http://scrapi.org/object/{objectid}";

var sourceCsv = "FlickrWithScrapi.csv";

var destCsv  = "FlickrScrapiWithImageUrl.csv";

var csv_parse = require("csv-parse");
var csv_transform = require("stream-transform");
var csv_stringify = require("csv-stringify");
var fs = require("fs");
var request = require("request")


var parser = csv_parse({delimiter: ','});

var skip = 0;


var flickrapikey = "2a738d26af4bf19c596490c1d422818e";

var Flickr = require("flickrapi");
var options = {api_key : flickrapikey};
var flickr;



var add_headers = function(orig_data, callback){
	orig_data.push("flickrid");
	orig_data.push("flickrpageurl");
	orig_data.push("flickrimageurl");
	orig_data.push("flickrimageurl_text");
	orig_data.push("username");
	orig_data.push("nsid");
	orig_data.push("pathalias");
	callback(null, orig_data);
}



var subarray = function(record, sub_term){
	var new_array = [];
	for(var i = 0; i<record.length; i++){
		new_array.push(record[i][sub_term]);
	}
	return new_array;
}

var first = true;
var rowcount= 0;
var transformer = csv_transform(function(record, callback){
	console.log(record[1]);
	if(first){
		first = false;
		add_headers(record, callback);
		return;
	} 

	var filename = record[1];
	var flickrid = filename.split(".")[0];
	console.log(flickrid);


	rowcount++;
	if(rowcount <= skip){
		console.log("skipping " +rowcount);
		callback(null);
		return;
	}

//https://c2.staticflickr.com/6/5064/5788415389_219109d356_b.jpg
// farm/server/id_secret_b.jpg 


	(function(_record, _callback, _flickrid){
		flickr.photos.getInfo({
			photo_id : _flickrid
		}, function(err, result){
			if(err){
				console.log("flickr error: " + err);
				callback(null, record);
				return;
			}


			console.log("flickr result");
			console.log(result);
			console.log(result.photo.urls);


			var farm = result.photo.farm;
			var server = result.photo.server;
			var secret = result.photo.secret;
//			var originalformat = result.photo.originalformat;
			var imageurl = "http://c2.staticflickr.com/"+farm+"/"+server+"/"+_flickrid+"_"+ secret+".jpg";
			console.log(imageurl);
			_record.push(_flickrid);
			_record.push(result.photo.urls.url[0]._content);
			_record.push(imageurl);
			_record.push(imageurl);
			_record.push(result.photo.owner.username);
			_record.push(result.photo.owner.nsid);
			_record.push(result.photo.owner.path_alias);

			var fs = require('fs');
				fs.writeFile("./flickrJson/"+_flickrid+".json", JSON.stringify(result, null,"  "), function(err) {
				    if(err) {
				        console.log(err);
				    } else {
				        console.log("The file was saved!");
				    }
			}); 


			callback(null, _record);
		});

	})(record, callback, flickrid);


},{parallel : 20});

var stringifier= csv_stringify({delimiter: ","});






transformer.on("error", function(err){
	console.log("error in transformer");
	console.log(err);
});
transformer.on("finish", function(){
	console.log("finished");
});	


Flickr.tokenOnly(options, function (error, flickrres){
	console.log("got flickr stuff");
	flickr = flickrres;
	fs.createReadStream(sourceCsv).pipe(parser).pipe(transformer).pipe(stringifier).pipe(fs.createWriteStream(destCsv));

});




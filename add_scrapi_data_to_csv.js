/*
processing csv file, running through scrapi to get additional data
*/

var scrapiSearch = "http://scrapi.org/search/{term}";

var scrapiObject = "http://scrapi.org/object/{objectid}";


var argv = require('minimist')(process.argv.slice(2));
var sourceCsv = argv._[0];
var destCsv = argv._[1];
console.log(argv);
console.log(sourceCsv);
console.log(destCsv);


var csv_parse = require("csv-parse");
var csv_transform = require("stream-transform");
var csv_stringify = require("csv-stringify");
var fs = require("fs");
var request = require("request")


var parser = csv_parse({delimiter: ','});

var skip = 0;


var combine_data = function(orig_data, new_data, callback){
	if(new_data.title){
		orig_data.push(new_data.title);
	}else{
		orig_data.push(" ");
	}
	if(new_data.primaryArtist){
		orig_data.push(new_data.primaryArtist.name);
	}else{
		orig_data.push(" ");
	}
	if(new_data.galleryLink){
		orig_data.push(new_data.galleryLink);
	}else{
		orig_data.push(" ");
	}
	if(new_data.gallery){
		orig_data.push(new_data.gallery);
	}else{
		orig_data.push(" ");	
	}
	if(new_data.inTheMuseumList && new_data.inTheMuseumList[0]){
		orig_data.push(new_data.inTheMuseumList[0].name);
		orig_data.push(new_data.inTheMuseumList[0].url);
	}else{
		orig_data.push(" ");
		orig_data.push(" ");
	}
	if(new_data.classificationList){
		orig_data.push(new_data.classificationList.join(","));
	}else{
		orig_data.push(" ");
	}
	if(new_data.whatList){
		orig_data.push(subarray(new_data.whatList, "name").join(","));
	}else{
		orig_data.push(" ");
	}
	orig_data.push(new_data.CRDID);
	orig_data.push(new_data.accessionNumber);
	orig_data.push("http://scrapi.org/object/"+new_data.CRDID);
	orig_data.push("http://www.metmuseum.org/collection/the-collection-online/search/"+new_data.CRDID);
	callback(null, orig_data);

}



var add_headers = function(orig_data, callback){
	orig_data.push("Title");
	orig_data.push("PrimaryArtistName");
	orig_data.push("galleryLink");
	orig_data.push("gallery");
	orig_data.push("InTheMuseumList Name");
	orig_data.push("InTheMuseumList Url");
	orig_data.push("ClassificationList");
	orig_data.push("whatList");
	orig_data.push("CRDID");
	orig_data.push("checkAccNo")
	orig_data.push("scrapiurl");
	orig_data.push("meturl");
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
	console.log(record[2]);
	var acc_no = record[2];
	if(first){
		first = false;
		add_headers(record, callback);
		return;
	} 

	rowcount++;
	if(rowcount <= skip){
		console.log("skipping " +rowcount);
		callback(null);
		return;
	}

	if(acc_no == "-" || !acc_no || acc_no.trim() == ""){
		callback(null, record);
		return;
	}

	console.log("acc_no " + acc_no);
	var length = record.length;
	console.log(length);

	(function(_record, _acc_no, _callback){
		var objSearch = scrapiSearch.replace("{term}", _acc_no); 

		console.log("searching " + objSearch);
		request(objSearch, function(error, resp, body){
		    if(error){
		      console.log("in call to Met Page, got error" + error );
		      _callback(null, _record);     
		      return;
		    }
		    var items = JSON.parse(body).collection.items;
		    if(items.length ==0){
		    	_callback(null, _record);
		    	return;
		    }
		    var i =0;


/*
// this needs some figuring out...
		    function matchAccNo(index, items, callback){
		    	if(index >= items.length){
		    		return false;
		    	}
		    	var url = items[index].href;

		    	request(__url, function(err2, resp2, body2){
				    if(error){
				      console.log("in call to Met Page, got error" + error );
				      __callback(null, __record);     
				      return;
				    }
				    var obj = JSON.parse(body2);
				    real_acc = obj.accessionNumber;
				    console.log("got object info : " + real_acc + " : " + __acc_no);
				    if(real_acc == __acc_no){
				    	console.log("this is the real object");
				    	combine_data(__record, obj, __callback);
				    	return;
				    }else{
				    	console.log("NOT matching accession number");
				    }
		    	});
		    }
*/
		    while(i < items.length){
		    	var url = items[i].href;
		    	console.log("url is " + url);
		    	i++;
		    	(function(__record, __acc_no, __url, __callback){
			    	request(__url, function(err2, resp2, body2){
					    if(error){
					      console.log("in call to Met Page, got error" + error );
					      __callback(null, __record);     
					      return;
					    }
					    if(!body2 || body2.trim() == "undefined" || body2.trim() == "Not Found"){
					      __callback(null, __record);     
					      return;
					    }
					    var obj = JSON.parse(body2);
					    real_acc = obj.accessionNumber;
					    console.log("got object info : " + real_acc + " : " + __acc_no);
					    if(real_acc == __acc_no){
					    	console.log("this is the real object");
					    	combine_data(__record, obj, __callback);
					    	return;
					    }else{
					    	console.log("NOT matching accession number");
					    }
			    	});
		    	}(_record, _acc_no, url, _callback));
		    }
		    callback(null);
		});
	}(record, acc_no, callback));
},{parallel : 20});

var stringifier= csv_stringify({delimiter: ","});


transformer.on("error", function(err){
	console.log("error in transformer");
	console.log(err);
});
transformer.on("finish", function(){
	console.log("finished");
});	


fs.createReadStream(sourceCsv).pipe(parser).pipe(transformer).pipe(stringifier).pipe(fs.createWriteStream(destCsv));

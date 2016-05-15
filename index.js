var fs = require("fs");
var util = require("util");
var opts;
var file_path = '';
var file_name = '';
var file_ext = '';
var save_path = '';
var log = [];
var l_num = 0;
var last_line = 0;
var lines = null;

function parseFile(file, cbFunc){
	write("Opening File...");
	OpenFile(file, 'r', function(err, fd){
		if(err){
			err = "Error: Opening file " + file + " (" + err + ")";
			write(err);
			cbFunc(err, null);
			return;
		}

		write("Getting file stats...")
		fs.fstat(fd, function(err, stats){
			if(err){
				err = "Error: Getting file stats. (" + err + ")";
				write(err);
				closeFile(fd, err, null, cbFunc);
				return;
			}

			write("File Size: " + stats.size);
			var buffer = new Buffer(stats.size);

			write("Reading file...");
			fs.read(fd, buffer, 0, stats.size, 0, function(err, bytesRead, buffer){
				if(err){
					err = "Error: Can not read file: " + err;
					write(err);
					closeFile(fd, err, null, cbFunc);
					return;
				}
				if(buffer.length < 20){
					err = "Error: File not valid, file length too short";
					write(err);
					closeFile(fd, err, null, cbFunc);
					return;
				}
				if(buffer.slice(0, 20).toString().indexOf("Binary") > -1){
					err = "Error: File is a FBX Binary file, can parse ASCII only";
					write(err);
					closeFile(fd, err, null, cbFunc);
					return;
				}
				
				parseText(buffer.toString(), function(err, obj){
					closeFile(fd, err, obj, cbFunc);
				});
			});
		});
	});	
}

function closeFile(fd, err, obj, cbFunc){
	write("Closing file...");
	fs.close(fd, function(c_err){
		cbFunc(err, obj);
	})
}

function parseFileSync(file){
	var err = '';

	write("Opening file...");
	var fd = fs.openSync(file, 'r');
	if(fd === null || fd == undefined){
		err = "Error: Opening file, " + file;
		return errorSync(fd, err);
	}

	write("Getting file stats...");
	var stats = fs.fstatSync(fd);
		
	if(stats === null || stats == undefined){
		err = "Error: Retrieving file stats.";
		return errorSync(fd, err);
	}

	write("File Size: " + stats.size);
	var buffer = new Buffer(stats.size);
	write("Reading file...");
	var bytesRead = fs.readSync(fd, buffer, 0, stats.size, 0);
	
	if(buffer.length < 20){
		err = "Error: Not a valid file, length too small";
		return errorSync(fd, err);
	}
	
	if(buffer.slice(0, 20).toString().toLowerCase().indexOf("binary") > -1){
		err = "Error: File is a Binary FBX file, only ASCII";
		return errorSync(fd, err);
	}

	var str = buffer.toString();

	write("Closing file...");
	fs.closeSync(fd);

	return parseText(str);
}

function errorSync(fd, err){
	write(err);
	fs.closeSync(fd);
	return {err: err, data: null};
}

function parseText(text, cbFunc){
	write("Parse Started...");
	var isAsync = isCallbackFunc(cbFunc);
	var err = null;
	var data = null;
	var num_lines = 0;
	lines = [];

	if(text && typeof text == "string"){
		var s_time = process.hrtime(); //start counter

		if(text.indexOf('\r\n') > -1){
			lines = text.split('\r\n');
		}
		else if(text.indexOf('\n') > -1){
			lines = text.split('\n');
		}

		num_lines = lines.length;
		last_line = num_lines - 1;
		
		if(num_lines > 0){
			data = processObject();
		}
		else{
			err = "Error: Can not split file data into lines.";
			write(err);
		}

		write("Parse Completed...");
		var d_time = process.hrtime(s_time);
		write("Parse time: " + d_time[0] + "s, " + d_time[1] + "ns");
		write("Number of Lines: " + num_lines);
	}
	else{
		err = "Error: No string passed to be parsed.";
		write(err);
	}

	if(isAsync){
		cbFunc(err, data);
	} else{
		return {err: err, data: data};
	}
}

function processObject(){
	var obj = {};
	var line = null;

	for(l_num; l_num <= last_line; l_num++){
		line = lines[l_num];
		line = line.trim();

		if(line == undefined || line == '' || line == null) continue;
		
		// Comment
		if(line.charAt(0) == ";"){
			if(opts.parseComments){
				if(!obj['comments']) obj['comments'] = [];
				var c = line.replace(';', '') + " (found @ line: " + l_num + ")";
				write("Parsing Comment: " + c);
				obj['comments'].push(c);
			}
		}
		// End of Object, return current object.
		else if(line.indexOf('}') > -1){
			return obj;
		}
		// Start of an Object
		else if(line.indexOf('{') > -1){
			var o = processObjectName(line);
			var n = o['name'];
			var g = o['group'];
			var t = o['type'];
			var oidx = g || n;
			var r_obj = null;

			write("Parsing Object: Name: " + n + ", Type: " + t + ", Group: " + g);
			
			l_num++;
			
			r_obj = processObject();

			if(t){
				r_obj["ObjectType"] = t;
			}

			if(g){
				if(!obj[g]) obj[g] = {};
				obj[g][n] = r_obj;
			}
			else{
				obj[n] = r_obj;
			}
		}
		// Key Value Pair
		else if(line.indexOf(':') > -1){
			var kv = null;
			if(line.indexOf('::') > -1){
				var idx = line.indexOf(':');
				kv = [line.slice(0, idx), line.slice(idx+1, line.length)];
			}
			else{
				kv = line.split(':');
			}
			
			write('Parsing Property "' + kv[0] + '": ' + kv[1]);

			var p = kv[0];
			var plc = p.toLowerCase();

			// Property Property (ex: "Property:....") -------------------
			if(plc == "property"){
				var pd = processPropertyData(kv[1]);
				obj[pd.name] = {type:pd.type, flags: pd.flags, value: pd.value};
			}
			// Connect Property ------------------------------------------
			else if(plc == "connect"){
				var cd = processConnectData(kv[1]);
				if(!obj[p]) obj[p] = [];
				obj[p].push(cd);
			}
			// Multi Value Property --------------------------------------
			else if(kv[1].indexOf(',') > -1){
				obj[p] = processMultiLineMultiValues(kv[1]);
			}
			// Property and Single Value ---------------------------------
			else{
				var k = kv[0];
				var v = kv[1];
				var pattern = /[:}]/;
				
				if(!v || v == ' ' || v == null || v == undefined){
					if(l_num < last_line && !pattern.test(lines[l_num + 1])){
						l_num++;
						v = lines[l_num];
						write('Values for Property "' + k + '" found on next line: ' + v);
						v = processMultiLineMultiValues(v);
					}
				}
				else{
					v = processValue(v);
				}
				
				obj[k] = v;
			}
		}
		// Catch All ---------------------------------------------------
		else{
			write("!!!Unprocessed Line (@ " + l_num + "): " + line);
		}
	} // end for loop

	return obj;
}

/* function to get the object name and possible type/description of the object.
line data passed in as a string, returns {name: str, type: str|null} */
// 1. ObjectName: { (ex: FBXHeaderExtension:)
// 2. ObjectGroup: ObjectName { (ex: ObjectType: "Geometry")
// 3. ObjectGroup: "ObjectGroup::ObjectName" { (ex: Model: "Model::Cube")
// 4. ObjectGroup: "ObjectGroup::ObjectName", "ObjectType" { (ex: Model: "Model::Producer Top", "Camera")
function processObjectName(s){
	var d = {name: '', type:null, group: null};
	if(s){
		s = s.replace('{', '');
		s = s.trim();
		var ss = s.split(':');

		d['group'] = ss[0];

		if(ss.length <= 2){ // #1 || #2
			if(!ss[1]){ // #1
				d['name'] = ss[0];
				d['group'] = null;
			} else { // #2
				d['name'] = ss[1];
			}
		}
		else{ // #3 || #4
			//ss = [0]=ObjectGroup, [1]=ObjectGroup, [2]=null, [3]=ObjectName (, ObjectType)
			if(ss[3].indexOf(',') > -1){ // #4
				var tnp = ss[3].split(',');
				tnp[1] = tnp[1].trim();
				d['name'] = tnp[0];
				d['type'] = tnp[1];
			}
			else{ // #3
				d['name'] = ss[3];
			}
		}

		for(i in d){
			if(d[i]){
				d[i] = d[i].replace(/"/g, '').replace(/ /, '');
			}
		}
	}

	return d;
}

function processPropertyData(p){
	//Property = Name, Type, Flags, Value(s)....,....
	var rp = {name: '', type: '', flags: '', value: null};
	if(p){
		p = cleanStr(p).replace(/, /g, ',');
		if(p.indexOf(',') > -1){
			var pv = p.split(',');
			var n = pv[0];
			var t = pv[1];
			var f = pv[2];
			var v = null;
			if(pv.length > 4){
				v = [];
				for(var i = 3; i < pv.length; i++){
					v.push(processValue(pv[i], t));
				}
			}
			else{
				var pvi = pv.length - 1; //parse value index
				v = processValue(pv[pvi], t);
			}

			rp['name'] = n;
			rp['type'] = t;
			rp['flags'] = f;
			rp['value'] = v;
		}
	}

	return rp;
}

function processConnectData(d){
	// type, source object/property, destination object/property
	// ex: "OO", "Model::Cube", "Model::Scene"
	var ro = {type:'', source:{type:'', name: null}, destination: {type:'', name:null}};
	if(d){
		var dp = cleanStr(d, true).split(',');
		var sdp = null;
		ro['type'] = dp[0];
		sdp = dp[1].split('::');
		ro['source']['type'] = sdp[0];
		ro['source']['name'] = sdp[1];
		sdp = dp[2].split('::');
		ro['destination']['type'] = sdp[0];
		ro['destination']['name'] = sdp[1];
	}

	return ro;
}

function processMultiLineMultiValues(d){
	var vs = [];
	var continued = false;
	var pattern = /[}:]/;
	do{
		continued = false;
		d = cleanStr(d.replace(/, /g, ','));
		if(d.charAt(d.length - 1) == ','){
			continued = true;
			d = d.slice(0, d.length - 1); // remove the last comma
		}
		d = d.split(',');
		for(var i = 0; i < d.length; i++){
			vs.push(processValue(d[i]));
		}

		if(continued && l_num < last_line && !pattern.test(lines[l_num+1])){
			l_num++;
			d = lines[l_num];
		}
	}while(continued);

	return vs;
}

function processValue(v, t){
	if(v != null && v != undefined){
		v = v.trim();

		switch(t){
			case "double":
			case "float":
			case "Real":
			case "real":
			case "Color":
			case "color":
			case "ColorRGBA":
			case "colorrgba":
				v = parseFloat(v);
				break;
			case "int":
			case "enum":
			case "bool":
			case "Vector3D":
			case "vector3d":
				v = parseInt(v);
				break;
			default:
				if(!isNaN(v)){
					if(v.indexOf('.') > -1){
						v = parseFloat(v);
					} else {
						v = parseInt(v);
					}
				}
				else{
					v = cleanStr(v);
				}
				break;
		}
	}
	else{
		write("!!!Unprocessed Value: " + v);
		v = null;
	}

	return v;
}

// cleans a string of quotes and leading and trailing spaces, option: removes all spaces
function cleanStr(s, rs){ //(s: string (string), rs: remove spaces (bool))
	if(s){
		s = s.replace(/"/g, '').trim();
		if(rs) s = s.replace(/ /g, '');
	}
	return s;
}

function OpenFile(file, option, cbFunc){
	if(isCallbackFunc(cbFunc)){
		if(!file) cbFunc("No file", null);
		fs.open(file, option, cbFunc);
	}
	else{
		if(!file) return false;
		return fs.openSync(file, option);
	}
}

function validateFile(file, cbFunc){
	var isAsync = isCallbackFunc(cbFunc);
	var err = false;

	if(!file || typeof file != "string"){
		err = "Error: No file provided or path not string.";

		if(isAsync){ cbFunc(err); }
		else{ return err; }
	}

	if(file.indexOf('.fbx') < 1){ // a .fbx is a vaild file name
		err = 'Error: Not a vaild FBX ASCII file: ' + file;
		
		if(isAsync){ cbFunc(err); }
		else{ return err; }
	}

	err = "Error: File doesn't exist: " + file;

	if(isAsync){
		fs.exists(file, function(exist){
			if(exist){
				err = false;
				parseFilePath(file);
			}

			cbFunc(err);
		});
	}
	else{
		if(fs.existsSync(file)){
			err = false;
			parseFilePath(file);
		}

		return err;
	}
}

function parseFilePath(file, cbFunc){
	var isAsync = isCallbackFunc(cbFunc);
	var err = false;

	if(!file || typeof file != "string"){
		err = "Error: No file path given.";
		if(isAsync){cbFunc(err);}
		else{return err;}
	}
	else{
		if(file.indexOf('/') == -1 || file.indexOf('.') == -1){
			err = "Error: Not a valid file path or file.";
			if(isAsync){cbFunc(err);}
			else{return err;}
		}
		var f = file.split('/');
		var fn = f[f.length-1].split('.');
		file_name = fn[0];
		file_ext = fn[1];
		file_path = f.slice(0, f.length - 1).join('/') + '/';
	}
}

function getDefaultOptions(){
	return  {
		parseComments: false,
		verbose: false,
		logging: false,
		returnJSON: false,
		saveJSON: false
	};
}

function setOptions(options){
	opts = getDefaultOptions();
	
	if(options && typeof options == "object"){
		if(options.parseComments == true){
			opts.parseComments = true;
		}
		if(options.verbose == true){
			opts.verbose = true;
		}
		if(options.logging == true){
			opts.logging = true;
		}
		if(options.returnJSON == true){
			opts.returnJSON = true;
		}
		if(options.saveJSON == true){
			opts.saveJSON = true;
		}
	}

	writeToConsole("Setting options...");
}

function writeLoggingHeader(){
	writeToLog("FBXASCIIToJs - .fbx to JS Parser.");
	writeToLog("Log file from parsing file: " + file_path + file_name + '.' + file_ext);
	writeToLog("Options: " + JSON.stringify(opts));
	writeToLog(" ");
	writeToLog("===============================================================================================================================");
	writeToLog(" ");
}

function write(msg){
	writeToConsole(msg);
	writeToLog(msg);
}

function writeToLog(msg){
	if(opts.logging && msg){
		log.push(msg);
	}
}

function writeToConsole(txt, override){
	if((opts.verbose || override) && txt){
		console.log(txt);
	}
}

function isCallbackFunc(cbFunc){
	return cbFunc && typeof cbFunc == "function";
}

function createFolder(cbFunc){
	var isAsync = isCallbackFunc(cbFunc);
	
	if(opts.saveJSON || opts.logging){
		write("Creating folder to save log and/or JSON files...");
		var path = file_path + file_name + '/';
		write("Checking if path '" + path + "' exists...");
		if(isAsync){
			fs.exists(path, function(exists){
				if(!exists){
					write("Creating folder '" + path + "'...");
					fs.mkdir(path, function(e){
						if(e){
							write("Error: Can not create folder " + path + " | " + e);
							write("Files will be saved to '" + file_path + "'...");
							save_path = file_path;
							cbFunc(e);
						}
						
						write("Folder created: " + path);
						save_path = path;
						cbFunc(0);
					});
				}
				else{
					write("Folder already exists, no need to create it...");
					save_path = path;
					cbFunc(0);
				}
			});
		}
		else{
			if(!fs.existsSync(path)){
				var e = fs.mkdirSync(path);

				if(e){
					write("Error: Can not create folder " + path + " | " + e);
					write("Files will be saved to " + file_path);
							save_path = file_path;
					return e;
				}
				else{
					write("Folder created: " + path);
					save_path = path;
					return 0;
				}
			}
			else{
				write("Folder already exists, no need to create it...");
				save_path = path;
				return 0;
			}
		}
	}
	else{
		if(isAsync){cbFunc(0);}
		else{return 0;}
	}
}

function processJSON(data, cbFunc){
	var isAsync = isCallbackFunc(cbFunc);
	var err = 0;

	if(opts.saveJSON || opts.returnJSON){
		if(data == null){
			err = "ERROR: Can not create JSON...  Data is null!";
			write(err);
			if(isAsync){cbFunc(err, null);}
			else{return null;}
		}
		else{
			writeToConsole("Creating JSON from data...");
			var json = JSON.stringify(data);

			if(opts.saveJSON){
				var jsonFile = save_path + file_name + ".json";
				writeToConsole("Saving JSON file...");

				if(isAsync){
					fs.writeFile(jsonFile, json, function(wErr){
						if(wErr){
							err = "Error: Saving JSON file: " + wErr;
							write(err);
						}
						else{
							write("JSON saved to file " + jsonFile);
						}
						cbFunc(err, opts.returnJSON ? json : null);
					});
				}
				else{
					try{
						fs.writeFileSync(jsonFile, json);
						write("JSON saved to file " + jsonFile);
					}
					catch(e){
						write("Error: Can not save JSON file: " + e);
					}

					return opts.returnJSON ? json : null;
				}
			}
			else{
				if(isAsync){cbFunc(0, json);}
				else{return json;}
			}
		}
	}
	else{
		if(isAsync){cbFunc(0, null);}
		else{return null;}
	}
}

function saveLog(cbFunc){
	var isAsync = isCallbackFunc(cbFunc);
	var err = 0;

	if(opts.logging){
		writeToConsole("Saving Log file...");
		var logFile = save_path + file_name + ".log";

		if(log != null && log.length > 0){
			var logData = log.join('\r\n');

			if(isAsync){
				fs.writeFile(logFile, logData, function(wErr){
					if(wErr){
						err = "Error: Can not save log file: " + wErr;
						writeToConsole(err);
					}
					else{
						writeToConsole("Log file saved: " + logFile);
					}

					cbFunc(err);
				});
			}
			else{
				try{
					fs.writeFileSync(logFile, logData);
					writeToConsole("Log file saved: " + logFile);
				}
				catch(e){
					err = "Error: Can not save log file: " + e;
					writeToConsole(err);
				}

				return err;
			}
		}
		else{
			err = "Error: No log data recorded to save.";

			if(isAsync){cbFunc(err);}
			else{return err;}
		}
	}
	else{
		if(isAsync){cbFunc(err);}
		else{return err;}
	}
}

function parse(file, options, cbFunc){
	var ov_s_time = process.hrtime();
	var s_mem = process.memoryUsage();

	if(arguments.length == 0){
		writeToConsole("Error: No arguments found.", true);
		process.exit(1);
	}
	
	if((arguments.length == 2 && typeof options != "function")||(arguments.length == 3 && typeof cbFunc != "function")){
		writeToConsole("Error: No callback function provided.", true);
		process.exit(1);
	}

	if(arguments.length == 2){
		cbFunc = options;
		options = {};
	}

	setOptions(options);

	validateFile(file, function(err){
		if(err){
			write(err);
			cbFunc(err, null);
		}
		else{
			writeLoggingHeader();

			parseFile(file, function(err, parsedObj){
				var rObj = {err: err, data: parsedObj};
				if(err){
					cbFunc(err, rObj);
					return;
				}
				createFolder(function(err){
					processJSON(parsedObj, function(err, json){
						if(!err && json != null){
							rObj.JSON = json;
						}

						write("Memory usage before parse: " + util.inspect(s_mem, {depth:null}));
						var u_mem = process.memoryUsage();
						write("Memory usage after parse: " + util.inspect(u_mem, {depth: null}));
						write("Total memory used: " + (u_mem.heapUsed - s_mem.heapUsed))

						var ov_e_time = process.hrtime(ov_s_time);
						write("Overall Run Time: " + ov_e_time[0] + "s, " + ov_e_time[1] + "ns");

						saveLog(function(err){
							cbFunc(err, rObj);
						});
					});
				});
			});
		}
	});
}

function parseSync(file, options){
	var rData = {err: '', data: null};
	var ov_s_time = process.hrtime();
	var s_mem = process.memoryUsage();
	
	setOptions(options);

	var err = validateFile(file);
	
	if(err){
		write(err);
		rData.err = err;
		return rData;
	}

	writeLoggingHeader();

	var parsedObj = parseFileSync(file);

	if(parsedObj.err){
		return parsedObj;
	}

	createFolder();

	var json = processJSON(parsedObj.data);

	write("Memory usage before parse: " + util.inspect(s_mem, {depth:null}));
	var u_mem = process.memoryUsage();
	write("Memory usage after parse: " + util.inspect(u_mem, {depth: null}));
	write("Total memory used: " + (u_mem.heapUsed - s_mem.heapUsed))

	var ov_e_time = process.hrtime(ov_s_time);
	write("Overall Run Time: " + ov_e_time[0] + "s, " + ov_e_time[1] + "ns");

	rData.err = err;
	rData.data = parsedObj;

	if(json) rData.json = json;

	saveLog();

	return rData;
}

exports.parse = parse;
exports.parseSync = parseSync;
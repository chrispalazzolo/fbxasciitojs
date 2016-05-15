fbxasciitojs
=======

A nodejs module to parses FBX/Filmbox (.fbx) ASCII files (not binary) to javascript or JSON.

Overview
--------
Parse FBX/Filmbox files (.fbx) to a javascript object and/or JSON.

Option to save the parsed data to a JSON file which will also break down the data to segments and save each segment to it's own JSON file.

Installation
------------
> npm install fbxasciitojs

Dependencies
------------
None

Usage:
-------
Returns a js object and optional JSON.

var parser = require("fbxasciitojs");

```javascript
// Async call
parser.parse(file, function(err, data){
  // do stuff with data
});
```
or

```javascript
//Synchronous call
var data = parser.parseSync("C:/3d/data/sample.fbx");
```

Methods
-------
parse - Is an async method which takes in a filepath and optional options object.  Returns a js objected of the parsed data. Options to save JSON and log information to a file. For options See below

`parse(file[,options], callback)`

parseSync - Is a synchronous call which takes the same arguments as the asynchronous parse call..

`parseSync(file[,options])`

Parameters
----------
These are the parameters for methods...

`parse(file, options, callback)` and `parseSync(file, options)`

file - <string> - full path to the .fbx file.
options - <object> - flags for events such as logging, saving to JSON files, etc...
callback - <function> - callback function only for the async call 'parse'.

Options
-------
Is a js object that contains configurations for logging, saving JSON files, etc...

parseComments - `bool` (default: `false`) - flag to retain comments when parsing the file.<br />
verbose - `bool` (default: `false`) - flag to write parsing details to the nodejs console.<br />
logging - `bool` (default: `false`) - flag to write and save parsing details to a log file.<br />
returnJSON - `bool` (default: `false`) - flag to have the a JSON string of the parsed object returned with the js object.<br />
saveJSON - `bool` (default: `false`) - flag to save the parsed data as a JSON string to file.

Default Option object if the options parameter is omitted...
```javascript
options = {
  parseComments: false,
  verbose: false,
  logging: false,
  returnJSON: false,
  saveJSON: false
}
```

License
-------
MIT

## Release History

* 0.0.0 Initial Release
* 0.1.0 Add binary file check and cleaned up error handling
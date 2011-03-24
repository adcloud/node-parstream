# Parstream JSON client

This is a client for the parstream db. This module at least requires
parstream v1.6.0 since it depends on the JSON interface.

## Install
Just install the module via npm.
<pre>
npm install parstream
</pre>

## Example

<pre>
var parstream = require('parstream').createClient('your.parstream-host.com', 9042);

parstream.query('SELECT * FROM foo WHERE bar="fast"', function(err, data) {
  // do something with your data...
})


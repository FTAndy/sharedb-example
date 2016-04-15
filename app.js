var ShareDB = require('sharedb');
var browserChannel = require('browserchannel').server
var Duplex = require("stream").Duplex
var serveStatic = require('serve-static')
var connect = require("connect");
var richText = require('rich-text')

ShareDB.types.register(richText.type)

var server = connect()

server.use(serveStatic(__dirname + "/public", {'index': ['quill.html']}));

// var db = require('mongodb').connect('mongodb://localhost:27017', function(err, mongo) {
//   if (err) throw err;
//   var db = require('sharedb-mongo')({mongo: mongo});
// });

// var backend = ShareDB({db: db});

var backend = ShareDB()

var numClients = 0

server.use(browserChannel({
  cors: "*",
  sessionTimeoutInterval: 5000
}, function(client) {
  var stream;
  numClients++;
  stream = new Duplex({
    objectMode: true
  });
  stream._write = function(chunk, encoding, callback) {
    console.log('s->c ', JSON.stringify(chunk));
    if (client.state !== 'closed') {
      client.send(chunk);
    }
    return callback();
  };
  stream._read = function() {};
  stream.headers = client.headers;
  stream.remoteAddress = stream.address;
  client.on('message', function(data) {
    // data = JSON.parse(data)
    console.log('c->s ', JSON.stringify(data));
    return stream.push(data);
  });
  stream.on('error', function(msg) {
    console.log('error')
    return client.stop();
  });
  client.on('close', function(reason) {
    stream.push(null);
    stream.emit('close');
    numClients--;
    return console.log('client went away', numClients);
  });
  stream.on('end', function() {
    return client.close();
  });
  return backend.listen(stream);
}));

var port = 8080;
server.listen(port, function() {
  return console.log("Listening on " + port);
});

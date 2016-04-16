### Mongo

install mongo and

```
$ mongod
```

### Install Browserify and Npm

```
$ npm install -g browserify
$ npm install
$ browserify node_modules/sharedb/lib/client/index.js --standalone sharedb > public/sharedb-client.js
```

and

```
$ node app.js
```

and open `localhost:8080`

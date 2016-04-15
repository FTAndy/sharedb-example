### Mongo

install mongo

```
$ mongod
```

### Install and Browserify

```
$ npm install
$ browserify node_modules/sharedb/lib/client/index.js --standalone sharedb > public/sharedb-client.js
```

and

```
$ node app.js
```

but still has bugs

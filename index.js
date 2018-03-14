var http = require('http'),
    express = require('express'),
    path = require('path'),
    MongoClient = require('mongodb').MongoClient,
    Server = require('mongodb').Server,
    CollectionDriver = require('./collectionDriver').CollectionDriver,
    bodyParser = require('body-parser'),
    async = require('async'),
    pubnub = require('pubnub').init({
      ssl: true,
      publish_key: "pub-c-0eaef818-7e8c-489c-a051-0803327ce0b8",
      subscribe_key: "sub-c-52ced5aa-53d9-11e4-820d-02ee2ddab7fe",
      uuid: 'server'
    }),
    mongoose = require('mongoose');

var app = express();

var jsonParser = bodyParser.json();
var urlencodedParser = bodyParser.urlencoded({extended:false});

app.set('port', process.env.PORT || 3000);
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');
// app.use(urlencodedParser);
app.use(jsonParser);

var mongoHost = 'localhost';
var mongoPort = 27017;
var collectionDriver;
mongoose.connect('mongodb://localhost/locations');

var mongoClient = new MongoClient(new Server(mongoHost, mongoPort));
mongoClient.open(function(err, mongoClient) {
   if (!mongoClient) {
      console.error("Error! Exiting.. Must start MongoDB first");
      process.exit(1);
   }
   var db = mongoClient.db("operators");
   collectionDriver = new CollectionDriver(db);
});

app.use(express.static(path.join(__dirname, 'public')));

function returnCollectionResults(req, res) {
  return function(error, objs) {
    if (error) {
      console.log(error);
      res.status(400).send(error);
    } else {
        res.set('Content-Type','application/json');
        res.status(200).send(objs);
    }
  };
};

var operatorAccepted = false;
var requestTimeout = 15000;
var requestTimeOutBuffer = 3000;

var unsubscribeTimeout = requestTimeout + requestTimeOutBuffer;

function sendJobRequestToOperators(requester, breakdownId, operators, callback) {

  if (operatorAccepted == false) {

    pubnub.subscribe({
      channel: operators,
      message: function(receivedMessage) {

        receivedMessage = JSON.stringify(receivedMessage);
        var message = JSON.parse(receivedMessage);

        var type = message.type;

        console.log(message.type);

        if (type) {

          if (type == 'accept') {
            callback(true);
          } else if (type == 'reject') {
            console.log(operators + ' rejected the request');
          }
        }

        unsubscribeFromChannels(operators, function(unsub_msg) {
          callback(false);
        });
        // callback(false);
      },
      connect: function() {

        setTimeout(function() {
          unsubscribeFromChannels(operators, function(unsub_msg) {
            console.log(operators + ' timed out');
            callback(false);
          });
        }, unsubscribeTimeout);

      },
      error: function(error) {
        callback(false);
      }
    });

    var requestMessage = {
      "aps": {
        "alert": "You have a new job request",
        "badge": 1,
        "sound": "default"
      },
      "type": "breakdown",
      "detail": {
        "breakdownId": breakdownId,
        "requestTimeout": requestTimeout
      }
    };

    sendMessageToOperator(requester, operators, requestMessage, function(response) {
      console.log(response);
    });

  }

}

function unsubscribeFromChannels(channels, callback) {

  pubnub.unsubscribe({
    channel: channels,
    callback: function(message) { 
      callback(message);
      console.log('===================================');
      console.log('unsubscribed from ' + channels);
      console.log('at ' + new Date());
      console.log('===================================');
    },
    error: function(error) {
      callback(error);
    }
  });
}

function sendMessageToOperator(from, to, message, callback) {

  pubnub.publish({
    channel: to,
    message: message,
    callback: function(msg) {
      console.log('***********************************');
      callback(msg);
      console.log('at ' + new Date());
      console.log('***********************************');
    },
    error: function(error) {
      callback(error);
    }
  })
};

var mongoosedb = mongoose.connection;
mongoosedb.on('error', console.error.bind(console, 'connection error:'));
mongoosedb.once('open', function (callback) {
  console.log('connected and opened');
});

var locationSchema = mongoose.Schema({
  locationName: String,
  radius: Number,
  centreCoordinate: {
      type: { type: String, default: 'Point' }, 
      coordinates: []
    },
  updatedAt: { type: Date, default: new Date() },
  status: String
});

locationSchema.index({ centreCoordinate: '2dsphere' });

var Location = mongoose.model('Location', locationSchema);

app.get('/activeLocations/:locations', function (req, res) {
  var params = req.params
  var locations = params.locations.split(',');
  var lat = Number(locations[0]);
  var lng = Number(locations[1]);

  var point = { type : "Point", coordinates : [lng,lat] };

  Location.geoNear(point, { maxDistance : 36000, spherical : true }, function (err, results, stats) {
    if (err) {
      res.status(200).send(err);
    } else {
      for (result in results) {
          console.log(results);
      }
      res.status(200).send({"results": results, "code": Number(200)});
    }
  });

});

app.get('/allLocations', function (req, res) {

  Location.find(function (error, results) {
    if (error) {
      res.status(204).send({"error": "no content", "code": Number(204)});
    } else {
      res.status(200).send({"results": results, "code": Number(200)});
    }
  });
});

app.get('/:collection', function(req, res, next) {
  var params = req.params;
  var query = req.query.query;
  if (query) {

    query = JSON.parse(query);
    collectionDriver.query(req.params.collection, query, returnCollectionResults(req, res));

  } else {

    collectionDriver.findAll(req.params.collection, returnCollectionResults(req, res));
  }
  
});

app.get('/:collection/available', function (req, res) {

  var params = req.params;
  var collection = params.collection;
  var query = req.query;
  var loc = query.loc;
  var rad = query.rad;
  var requester = query.req;

  console.log('requested by: ' + requester);

  if (loc) {

      var searchRadius;

      if (rad) {
        searchRadius = Number(rad);
      } else {
        searchRadius = 15;
      }

      collectionDriver.findAvailable(collection, loc, searchRadius, function (error, objs) {

          if (error) {
            console.log(error);
            res.status(400).send(error);
          } else {
            res.set('Content-Type','application/json');
            res.status(200).send(objs);
          }
      });

  } else {
    res.status(400).send({error: 'no location specified'})
  }

});

app.get('/:collection/request', function (req, res) {

  var params = req.params;
  var collection = params.collection;
  var query = req.query;
  var operators = query.operators.split(',');
  var requester = query.req;
  var breakdownId = query.breakdownId;

  console.log('---------------------------------');
  console.log('requested by ' + requester);
  console.log('requesting help from: ' + operators);
  console.log('---------------------------------');
  console.log('time is: ' + new Date());

  operatorAccepted = false;

  async.detectSeries(operators, sendJobRequestToOperators.bind(null, requester, breakdownId), function finalDetect(result) {

    if (result == undefined) {

      res.status(400).send({"error": "nobody answered your call"});
      console.log('---------------------------------');
      console.log('no-one answered or wanted the job at ' + new Date());
      console.log('---------------------------------');

    } else {

      operatorAccepted = true;

      res.status(200).send({"accepted": "true",
                            "operatorId": result,
                            "time": new Date()});

      console.log('+++++++++++++++++++++++++++++++++');
      console.log(result + ' accepted the request');
      console.log('at ' + new Date());
      console.log('+++++++++++++++++++++++++++++++++');

      collectionDriver.getByUserObjectId(collection, result, function (error, operatorObject) {

        var operatorObject = operatorObject[0];

        var coords = operatorObject.location.coordinates;

        var userNotification = {
          tender: {
            "breakdownId": breakdownId,
            "operatorId": result,
            "type": "accept",
            "latitude": coords[1],
            "longitude": coords[0],
            "totalTime": 25
          }
        }

        pubnub.publish({
          channel: requester,
          message: userNotification,
          callback: function (success_msg) {
            console.log(success_msg);
          },
          error: function (error) {
            console.log(error);
          }
        });
      });

    }

  });

});

app.get('/:collection/operator/:operatorId', function (req, res) {
  var params = req.params;
  var operatorId = params.operatorId;
  var collection = params.collection;
  if (operatorId) {
    collectionDriver.getLatest(collection, operatorId, function(error, objs) {
      if (error) {
        res.status(400).send(error);
      } else {
        res.status(200).send(objs);
      }
    });
  } else {
    res.status(400).send({error: 'bad url', url: req.url});
  }
});

app.get('/:collection/list/:operatorId', function (req, res) {

  var params = req.params;
  var collection = params.collection;
  var operators = params.operatorId.split(',');

  if (operators) {
      
      collectionDriver.getLatestWithArray(collection, operators, function(error, object) {

        if (error) {
          res.status(400).send(error);
        } else {
          res.status(200).send(object);
        }

      });

  } else {
    res.status(400).send({error: 'bad list', list: operators});
  }

});

app.get('/:collection/:entity', function(req, res) {
  var params = req.params;
  var entity = params.entity;
  var collection = params.collection;
  if (entity) {
    collectionDriver.get(collection, entity, function(error, objs) {
      if (error) {
        res.status(400).send(error);
      } else {
        res.status(200).send(objs);
      }
    });
  } else {
    res.status(400).send({error: 'bad url', url: req.url});
  }
});

app.post('/:collection', function(req, res) {
  var object = req.body;
  var collection = req.params.collection;
  collectionDriver.save(collection, object, function (error, docs) {
    if (error) {
      res.status(400).send(error);
    } else {
      res.status(201).send(docs);
    }
  });
});

app.put('/:collection/:entity', function(req, res) {
  var params = req.params;
  var entity = params.entity;
  var collection = params.collection;

  if (entity) {
    collectionDriver.update(collection, req.body, entity, function(error, objs) {
      if (error) {
        res.status(400).send(error);
      } else {
        res.status(200).send(objs);
      }
    });
  } else {
    var error = { "message" : "Cannot PUT a whole collection" };
    res.status(400).send(error);
  }
});

app.delete('/:collection/:entity', function(req, res) {
  var params = req.params;
  var entity = params.entity;
  var collection = params.collection;

  if (entity) {
    collectionDriver.delete(collection, entity, function(error, objs) {
      if (error) {
        res.status(400).send(error);
      } else {
        res.status(200).send(objs);
      }
    });
  } else {
    var error = { "message" : "Cannot DELETE a whole collection"};
    res.status(400).send(error);
  }
});

app.use(function (req, res) {
   res.render('404', { url: req.url });
});

http.createServer(app).listen(app.get('port'), function() {
   console.log('Express server listening on port ' + app.get('port'));
});

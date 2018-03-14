var mongo = require('mongodb');
var ObjectID = mongo.ObjectID;


CollectionDriver = function(db) {
   this.db = db;
}

// Get the Collection name for other functions 
CollectionDriver.prototype.getCollection = function(collectionName, callback) {
   this.db.collection(collectionName, function(error, currentCollection) {
      if (error) {
         callback(error);
      } else {
         callback(null, currentCollection);
      }
   });
};

// Get all items in a collection
CollectionDriver.prototype.findAll = function(collectionName, callback) {
   this.getCollection(collectionName, function(error, currentCollection) {
      if (error) callback(error);
      else {
         currentCollection.find().toArray(function(error, results) {
         if (error) callback (error);
            else callback(null, results);
         });
      }
   });
};

CollectionDriver.prototype.findAvailable = function(collectionName, latLng, radius, callback) {
   this.getCollection(collectionName, function(error, currentCollection) {

      if (error) {
         callback(error);
      } else {

         var drivingDistance = require('google-distance-matrix');
         drivingDistance.key('AIzaSyBgWcP_KMW0768_lLCI7ftDZdWH8jzj3vc');
         drivingDistance.mode('driving');

         var location = latLng.split(',');
         var lat = Number(location[0]);
         var lng = Number(location[1]);

         var distance = radius * 1609.34;

         currentCollection.aggregate([
         {
            "$geoNear": {
               near: {
                  type: "Point",
                  coordinates: [lng, lat]
               },
               distanceField: "distance.crowFliesMeters",
               maxDistance: distance,
               query : {
                  status: "available"
               },
               spherical: true
            }
         }], function(error, results) {

            if (error) {

               console.log("Mongo geoNear query FAIL: " + error);
               callback(error);
            } else {

               console.log("Mongo geoNear query OK");

               var destinations = [];

               // var previousId = "";
               // var trimmedResults

               for (result in results) {

                  // var currentId = results[result].userId;
                  // if (!currentId == previousId) {
                     var strLat = results[result].location.coordinates[1];
                     var strLng = results[result].location.coordinates[0];
                     destinations.push(strLat + "," + strLng);
                  // }

                  // previousId = currentId;
               }

               console.log(destinations);

               var coordinate = [];
               coordinate.push(location[0] + "," + location[1]);

               drivingDistance.matrix(coordinate, destinations, function(err, distances) {

                  if (err) {
                     return console.log("Google driving distancy query FAIL: " + err);
                  }

                  if (!distances) {
                     return console.log('Google driving distancy query FAIL: No driving distances available');
                  }

                  if (distances.status == 'OK') {

                     console.log("Google driving distancy query OK");
                     for (i = 0; i < destinations.length; i++) {

                        var element = distances.rows[0].elements[i];

                        if (element.status == 'OK') {

                           console.log("Operator " + i + " driving distance returned OK");

                           results[i].distance.drivingMeters = element.distance.value;  
                           results[i].distance.drivingTimeInSeconds = element.duration.value;

                        } else if (element.status == 'ZERO_RESULTS' ) {

                           console.log("Operator " + i + " driving distance not possible (travels overseas?) FAIL");

                        } else {

                           console.log("Operator " + i + " driving distance returned FAIL: " + element.status);
                        }
                     }
                  }

                  callback(null, results);
               });
            }
         });
      }
   });
};

// Get last 'operatorId' in a collection ordered descending
CollectionDriver.prototype.getLatest = function(collectionName, operatorId, callback) {
   this.getCollection(collectionName, function(error, currentCollection) {
      if (error) {
         callback(error);
      } else {
         currentCollection.find( { $query : {'operatorId' : operatorId}, $orderby: { updatedAt: -1 } } ).limit(1).toArray(function(error, results) {
            if (error) {
               callback(error);
            } else {
               callback(null, results);
            }
         });
      }
   });
}

// Get last 'operatorId' in a collection ordered descending
CollectionDriver.prototype.getByUserObjectId = function(collectionName, userObjectId, callback) {
   this.getCollection(collectionName, function(error, currentCollection) {
      if (error) {
         console.log(error);
         callback(error);
      } else {
         currentCollection.find( {'userId': userObjectId}).toArray(function(error, results) {
            if (error) {
               console.log(error);
               callback(error);
            } else {
               console.log(results);
               callback(null, results);
            }
         });
      }
   });
}

// Testing...
CollectionDriver.prototype.getLatestWithArray = function(collectionName, operatorIds, callback) {
   this.getCollection(collectionName, function(error, currentCollection) {

      if (error) {
         callback(error);
      } else {

         currentCollection.find( { operatorId : { $in: operatorIds } } ).sort( { operatorId: 1, updatedAt: -1 }).toArray(function(error, results) {
            if (error) {
               callback(error);
            } else {
               callback(null, results);
            }
         });

      }
   })
}
// End testing...

// Get items matching 'id' from a collection
CollectionDriver.prototype.get = function(collectionName, id, callback) {
   this.getCollection(collectionName, function(error, currentCollection) {
      if (error) callback(error);
      else {
         var checkForHexRegExp = new RegExp("^[0-9a-fA-F]{24}$");
         if (!checkForHexRegExp.test(id)) callback({error: "invalid id"});
         else currentCollection.findOne({'_id':ObjectID(id)}, function(error, doc) {
            if (error) callback(error);
            else callback(null, doc);
         });
      }
   });
}

// Save 'obj' to the collection
CollectionDriver.prototype.save = function(collectionName, obj, callback) {
   this.getCollection(collectionName, function(error, currentCollection) {

      if (error) {
         callback(error);
      } else {
         now = new Date();
         obj.updatedAt = now;
	      obj.createdAt = now;
         currentCollection.insert(obj, function() {
            callback(null, obj);
         });
      }
   });
}

// Update a specific 'obj' with the objectId
CollectionDriver.prototype.update = function(collectionName, obj, objectId, callback) {
   this.getCollection(collectionName, function(error, currentCollection) {
      if (error) {
         callback(error);
      } else {
         obj._id = ObjectID(objectId);
         now = new Date();
         obj.updatedAt = now;
         obj.createdAt = new Date();
         currentCollection.save(obj, function(error, doc) {
            if (error) {
               console.log('Updating error');
               callback(error);
            } else {
               callback(null, obj);
            }
         });
      }
   });
}

// Delete a speific 'objectId'
CollectionDriver.prototype.delete = function(collectionName, objectId, callback) {
   this.getCollection(collectionName, function(error, currentCollection) {
      if (error) {
         callback(error);
      } else {
         currentCollection.remove({'_id':ObjectID(objectId)}, function(error, doc) {
            if (error) {
               callback(error);
            } else {
               callback(null, doc);
            }
         });
      }
   });
}

// Perform a collection query
CollectionDriver.prototype.query = function(collectionName, query, callback) {
   this.getCollection(collectionName, function(error, currentCollection) {
      if (error) {
         callback(error);
      } else {
         currentCollection.find(query).toArray(function(error, results) {
            if (error) {
               callback(error);
            } else {
               callback(null, results);
            }
         });
      }
   });
}
exports.CollectionDriver = CollectionDriver;








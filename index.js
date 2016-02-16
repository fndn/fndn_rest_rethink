// Generic RethinkDB-backed JSON-speaking REST-api that supports
// - schema-less collections (e.g. tables)
// - image upload / download
// - diff (send me yours, I will send you mine)

var chalk 		= require('chalk');
var omitDeep 	= require('omit-deep');
var r 			= require('rethinkdb');
var bodyParser	= require('body-parser');

var JSONPH 		= require('json-parse-helpfulerror');
var json2csv 	= require('json2csv');
var flatKeys 	= require('recursive-keys').dumpKeysRecursively;


var default_conf = {
	db: 			'test',
	host: 			'localhost',
	port: 			28015,
	collections: 	['users'],
	http_port: 		9999,
	post_size_limit:'256mb',
};

module.exports.init = function( app, _conf ){
	
	/// merge conf with default conf
	var conf = default_conf;
	Object.keys(_conf).forEach( function(k){
		conf[k] = _conf[k];
	});
	console.log('REST, using conf', conf);
	
	_init_express(app, conf);

	_init_rethink(conf, function(err, connection){
		if (err) throw err;

		connection.use(conf.db);

		for(var i in conf.collections){
			expose_model(app, connection, '/api/', conf.collections[i] );
		}
	});

}

function expose_model(app, conn, prefix, model){
	console.log( chalk.green('+ exposing ')+ model +' on '+ prefix + model);

	
	// ensure the $model has a table/collection. create if not.
	r.tableList().run(conn, function(err, list) {
		if (err) throw err;

		if( list.indexOf(model) < 0 ){
			r.tableCreate(model).run(conn, function(err, result) {
				if (err) throw err;
				console.log('table-created:', model);
			});
		}
	});

	
	// init routes
	console.log( chalk.green('+ exposing ')+ model +' on '+ prefix + model);

	var R = r.table(model);

	/// all
	app.get(prefix + model, function(req, res){
		R.run(conn, res.many);
    });

	/// one
	app.get(prefix + model +'/:id', function(req, res){
		R.get(req.params.id).run(conn, res.one);
	});

	/// Return all IDs in the collection
	app.post(prefix + model +'/ids', function(req, res, next){
		R.pluck(['id']).run(conn, res.many);
	});

	/// update
	app.put(prefix + model +'/:id', function(req, res, next){
		R.get(req.params.id).update(req.body.doc).run(conn, res.one);
	});
	
	/// delete
	app.delete(prefix + model +'/:id', function(req, res, next){
		R.get(req.params.id).delete().run(conn, res.one);
	});
	
	/// create
	/// inserts or updates ("upserts") (if ID is already present).
	/// works with multiple docs if doc is an array (ex: doc = [{"id":5, "name":"b5"}, {"id":6, "name":"b6"}] )
	app.post(prefix + model, function(req, res, next){
		//console.log('R create', req.body);

		checkDuplicates( req.body, function(docs){
			
			R.insert( docs, {return_changes:'always', conflict:'update'}).run(conn, function(err, result){
				if( err ){
					res.status(500).json({error:err}).end();
					return;
				}

				// reply with received IDs only
				var ids = docs.map( function(doc){
					return doc.id;
				}).filter( function(id){
					return id.length==10;
				});
				
				res.status(200).json(ids).end();
			});

		});

		/*
		R.insert( req.body, {return_changes:'always', conflict:'update'}).run(conn, function(err, result){
			
			if( err ){
				res.status(500).json({error:err}).end();
				return;
			}

			// reply with received IDs only
			var ids = req.body.map( function(doc){
				return doc.id;
			}).filter( function(id){
				return id.length==10;
			});
			
			res.status(200).json(ids).end();
		});
		*/
	});

	/// diff
	/// Return all docs *not* present in doc.ids (ex: doc = {"ids":[1,3]} will return record 2, 4...
	/// RQL: r.db("expose06").table("brand").filter( function(v){ return r.not(r.expr([1,3]).contains(v('id'))) })
	app.post(prefix + model +'/diff', function(req, res, next){
		//console.log('R diff', req.body);
		if( JSON.stringify(req.body) == "{}" ){
			R.run(conn, res.many);
		}else{
			R.filter( function(v){ return r.not(r.expr(req.body).contains(v('id'))) }).run(conn, res.many);
		}

	});

	// csv export
	app.get('/csv/' + model, function(req, res){
		R.run(conn, function(err, cursor){
			if( err ){
				res.status(500).json({error:err}).end();
			}else{
				cursor.toArray(function(err, result) {
					
					var fields = flatKeys( result[0] );
					console.log('fields', fields);

					//res.csv(result, null, fields, true);
					json2csv({data:result, fields:fields, flatten:true, defaultValue:'n/a', del:';'}, function(err, csv) {
						if (err) console.log(err);
						
						res.csv( '', csv);
					});
				});
			}
		});
    });
}

function _init_rethink(conf, cb){
	r.connect( conf, function(err, conn){
		if( err ) return cb(er, null);
		
		r.dbList().run(conn, function(err, list) {
			if (err) throw err;

			if( list.indexOf(conf.db) < 0 ){
				r.dbCreate(conf.db).run(conn, function(err, res){
					console.log( chalk.green('+ Created db ')+ conf.db);
					return cb(err, conn);
				});
			}else{
				console.log( chalk.green('Using db ')+ conf.db);
				return cb(null, conn);
			}
		});
	});
}

function _init_express(app, conf){

	app.use(function(req, res, next){
		res.csv = function(filename, body){
			this.charset = 'utf-8';
			this.header('Content-Type', 'text/csv');
			this.header('Content-disposition', 'attachment; filename=' + filename);
			return this.send(body);
		}
		next();
	});

	// parse application/x-www-form-urlencoded
	app.use(bodyParser.urlencoded({ extended: false, limit: conf.post_size_limit }));

	// parse application/json
	app.use(bodyParser.json({limit: conf.post_size_limit, strict:false}));

	app.use(function(req, res, next){
		
		res.many = function(err, cursor){
			if( err ){
				res.status(500).json({error:err}).end();
			}else{
				cursor.toArray(function(err, result) {
					res.status(200).json(result).end();
				});
			}
		}

		res.one = function(err, result){
			if( err ){
				res.status(500).json({error:err}).end();
			}else{
				res.status(200).json(result).end();
			}
		}

		next();
	});

	app.post('*', checkJSON);
	app.put('*',  checkJSON);
}

/// utils

function checkDuplicates(_docs, cb){ // docs MUST be an array of objects
	var docs = [];
	var count = _docs.length;
	var i = 0;

	console.log("[checkDuplicate] ", count, typeof _docs );

	_docs.forEach( function(doc){
		var simpledoc = omitDeep(doc, ['_id', 'id', 'created_at', 'gps', 'reporter', 'price']);
		R.filter(simpledoc).count().run(conn, function(err, result){
			console.log('duplicate ?', result);
			if( result === 0 ){
				docs.push(doc);
			}else{
				console.log('-> discarding ', simpledoc);
			}
			i++;
			if( i === count ){
				// done
				cb(docs);
			}
		});
	});
}

var checkJSON = function(req, res, next){
	//console.log('checking JSON');
	//console.log('req.body', req.body);

	req.body = asArray(req.body);

	next();
}

// { '0': { name: 'ALB1' }, '1': { name: 'ALB2' }, '2': { name: 'ALB3' } };
var isObjectArray = module.exports.isObjectArray = function(obj){
	var i=0, arr=false;
	for( var k in obj){ arr = k == i++; }
	return arr;
}

// [ { name: 'ALB1' }, { name: 'ALB2' }, { name: 'ALB3' } ]
var fixObjectArray = module.exports.fixObjectArray = function(obj){
	var arr = [];
	for( var k in obj){
		arr.push( obj[k] )
	}
	return arr;
}

var asArray = module.exports.asArray = function(obj){
	//console.log('1 asArray', obj);
	if( !isObjectArray(obj) ){
		return obj;
	}else{

		var c = fixObjectArray(obj);
		//console.log('2 asArray', c);

		return c;
	}
}

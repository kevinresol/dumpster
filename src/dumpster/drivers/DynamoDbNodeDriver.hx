package dumpster.drivers;

import haxe.DynamicAccess;
import dumpster.AST;

class DynamoDbNodeDriver implements Driver {
  
  var engine:QueryEngine;
  var dynamodb:DynamoDb;
  
  public function new(?options:{dynamodb:{}, ?engine:QueryEngine}) {
    this.engine = switch options.engine {
      case null: new dumpster.QueryEngine.SimpleEngine();
      case v: v;
    }
    dynamodb = new DynamoDb(options.dynamodb);
  } 
  
  public function get<A>(id:Id<A>, within:CollectionName<A>):Promise<Document<A>> {
    return assertTable(within).next(function(_) {
      return Future.async(function(cb) {
        dynamodb.getItem({
          Key: {id: {S: id.toString()}},
          TableName: within,
        }, function(err, data) cb(if(err != null) Failure(Error.ofJsError(err)) else Success(data)));
      });
    });
  }
  
  public function find<A>(within:CollectionName<A>, check:ExprOf<A, Bool>, ?options:{ ?max:Int }):Promise<Array<Document<A>>> {
    return tableExists(within).next(function(_) {
      return Future.async(function(cb) {
        var param = toParam(within, check);
        dynamodb.scan(param, function(err, data) cb(if(err != null) Failure(Error.ofJsError(err)) else Success(data)));
      });
    });
  }
  
  public function count<A>(within:CollectionName<A>, check:ExprOf<A, Bool>):Promise<Int> {
    return tableExists(within).next(function(_) {
      throw 'not implemented count';
    });
  }
  
  public function set<A>(id:Id<A>, within:CollectionName<A>, doc:ExprOf<A, A>, ?options:{ ?ifNotModifiedSince:Date, ?patiently:Bool }):Promise<{ before: Option<Document<A>>, after: Document<A> }> {
    try {
      var data = engine.compile(doc)(null);
      return ensureTable(within).next(function(_) {
        return Future.async(function(cb) {
          dynamodb.putItem({
            Item: {
              id: {S: id.toString()},
              created: {S:"Now"},
              modified: {S:"Now"},
              data: {S:"Testing"},
            },
            TableName: within,
          }, function(err, data) {
            cb(if(err != null) Failure(Error.ofJsError(err)) else Success(data));
          });
        });
      });
    } catch(e:Dynamic) return Error.withData(500, Std.string(e), e);
  }
  
  public function update<A>(within:CollectionName<A>, check:ExprOf<A, Bool>, doc:ExprOf<A, A>, ?options:{ ?ifNotModifiedSince:Date, ?patiently:Bool, ?max:Int }):Promise<Array<{ before:Document<A>, after:Document<A> }>> {
    return tableExists(within).next(function(_) {
      throw 'not implemented update';
    });
  }
  
  public function shutdown():Promise<Noise> {
    return Noise;
  }
  
  var caches = {
    createTable: new Map<String, {result: Promise<Noise>, date:Date}>(),
    tableExists: new Map<String, {result: Promise<Bool>, date:Date}>(),
  }
  
  function createTable(name:String):Promise<Noise> {
    var now = Date.now();
    if(!caches.createTable.exists(name) || caches.createTable.get(name).date.getTime() < now.getTime() - 500000) {
      var result = Future.async(function(cb) {
        dynamodb.createTable(
          {
            AttributeDefinitions: [{
              AttributeName: 'id', 
              AttributeType: 'S',
            }],
            KeySchema: [{
              AttributeName: 'id', 
              KeyType: 'HASH',
            }],
            ProvisionedThroughput: {
              ReadCapacityUnits: 5, 
              WriteCapacityUnits: 5,
            },
            TableName: name,
          },
          function(err, _) cb(if(err != null) Failure(Error.ofJsError(err)) else Success(Noise))
        );
      });
      caches.createTable.set(name, {result: result, date: now});
    }
    return caches.createTable.get(name).result;
  }
  
  function ensureTable(name:String):Promise<Noise> {
    return tableExists(name).next(function(exists) return if(exists) Noise else createTable(name));
  }
  
  function assertTable(name:String):Promise<Noise> {
    return tableExists(name).next(function(exists) return if(exists) Noise else new Error(NotFound, 'Collection $name does not exist'));
  }
  
  function tableExists(name:String):Promise<Bool> {
    var now = Date.now();
    if(!caches.tableExists.exists(name) || caches.tableExists.get(name).date.getTime() < now.getTime() - 500000) {
      var result = Future.async(function(cb) {
        dynamodb.describeTable({
          TableName: name,
        }, function(err, data) {
          cb(
            if(err == null) 
              Success(true)
            else if(untyped err.code == 'ResourceNotFoundException')
              Success(false)
            else 
              Failure(Error.ofJsError(err)) 
          );
        });
      });
      caches.tableExists.set(name, {result: result, date: now});
    }
    return caches.tableExists.get(name).result;
  }
  
  function toParam<A, R>(name:String, expr:ExprOf<A, R>) {
    return new QueryParamBuilder(expr).toParams(name);
  }
}

@:jsRequire('aws-sdk', 'DynamoDB')
extern class DynamoDb {
  function new(?config:{});
  function describeTable(params:{}, cb:js.Error->Dynamic->Void):Void;
  function createTable(params:{}, cb:js.Error->Dynamic->Void):Void;
  function putItem(params:{}, cb:js.Error->Dynamic->Void):Void;
  function getItem(params:{}, cb:js.Error->Dynamic->Void):Void;
  function scan(params:{}, cb:js.Error->Dynamic->Void):Void;
}

class QueryParamBuilder {
  var expression:String;
	var counter = 0;
	var names = new DynamicAccess();
	var values = new DynamicAccess();
    
	public function new(expr) {
		expression = rec(expr);
	}
	
	function rec<A, R>(e:ExprOf<A, R>) {
		return switch e {
			case EConst(value):
				var id = 'values${counter++}';
				var rep = new DynamicAccess();
        var type = getType(value);
				rep.set(type, value);
				values.set(id, rep);
				':$id';
			case EField(target, name):
        // TODO: handle target
				var id = 'names${counter++}';
				names.set(id, 'data.$name');
				'#$id';
			case EUnop((_:Unop<Dynamic, Dynamic>) => op, e):
        switch op {
          default: throw '$op not implemented'; 
        }
			case EBinop(op, e1, e2):
				rec(e1) + binop(op) + rec(e2);
      default:
        throw 'not implemented $e';
		}
	}
  
  function getType(v:Dynamic) {
    return
      if(Std.is(v, String)) 'S'
      else if(Std.is(v, Float)) 'N'
      else throw 'Cannot recognize type of $v';
  }
	
	function binop(op:Binop<Dynamic, Dynamic, Dynamic>):String
		return switch op {
			case Eq: ' = ';
      case Neq: ' <> ';
      case Gt: ' > ';
      case Gte: ' >= ';
      case Lt: ' < ';
      case Lte: ' <= ';
      case Or: ' OR ';
      case And: ' AND ';
      default: throw '$op not implemented';
		}
	
	public function toParams(tableName:String) {
		return {
			TableName: tableName,
			ExpressionAttributeNames: names,
			ExpressionAttributeValues: values,
			KeyConditionExpression: expression,
			// FilterExpression: expression,
		}
	}
}
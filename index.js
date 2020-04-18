const schm = require ('schm');
const _id = require ('uuid/v4');
const AWS = require ('aws-sdk');
const docClient = new AWS.DynamoDB.DocumentClient ();
const TableName = process.env.DB;

// root Model class (abstract class)
class Model {
  // constructor takes a a schema and type name
  // creates methods necessary for all models
  constructor (schema=schm ({}), type, obj={}, refs=[], created=false) {
    this.schema = schema;
    this.type = type;
    this.obj = schema.parse (obj);
    this.id = created ? obj.id : _id ();
    this.createdAt = new Date ().toString ();
    this.modifiedAt = new Date ().toString ();
    this.created = created;
    this.lock = false;
    this.populated = {};
    this.refs = refs;
  }
  // async write to db
  async save () {
    // if locked, stop execution, else lock
    if (this.lock) return new Promise (resolve => resolve ([new Error ('locked'), null]));
    this.lock = true;
    // if the record exists in the db, update by id
    if (this.created) {
      return new Promise (async (resolve, reject) => {
        try {
          await this.schema.validate (this.obj);
          let keys = Object.keys (this.schema.params).filter (k => this.obj [k]);
          // set up params
          let params = {TableName};
          params.Key = {id: this.id};
          params.UpdateExpression = 'set ' + keys.map (k => `#${k} = :${k}`).join (', ');
          params.UpdateExpression += ', #modifiedAt = :modifiedAt';
          params.ExpressionAttributeNames = keys.reduce ((acc, val) => {
            return {...acc, [('#' + val)]: val};
          }, {'#modifiedAt': 'modifiedAt'});
          params.ExpressionAttributeValues = keys.reduce ((acc, val) => {
            return {...acc, [(':' + val)]: this.obj [val]};
          }, {':modifiedAt': JSON.stringify (new Date ())});
          docClient.update (params, (err, data) => {
            this.lock = false;
            if (err) return reject (err);
            resolve (data);
          })
        } catch (e) {
          reject (e);
        }
      });
    }
    // first save - create the item in the table
    return new Promise (async (resolve, reject) => {
      try {
        // ensure type
        await this.schema.validate (this.obj);
        let Item = {...this.obj, type: this.type, id: this.id, createdAt: this.createdAt, modifiedAt: this.modifiedAt};
        docClient.put ({TableName, Item}, (err, data) => {
          if (err) resolve ([err, null]);
          this.created = true;
          this.lock = false;
          resolve (data);
        })
      } catch (e) {
        reject (e);
      }
    });
  }
  // async delete from db
  async del () {
    let params = {
      TableName,
      Key: {
          id: this.id
      },
      ConditionExpression: "id = :id",
      ExpressionAttributeValues: {
          ':id': this.id
      }
    }
    return docClient.delete (params).promise ();
  }
  // find all of this type method
  static find (obj) {
    const types = require ('./types');
    // if nothing is specified, return every document
    if (!obj) {
      return new Promise (async (resolve, reject) => {
        docClient.scan ({TableName}, (err, data) => {
          if (err) return reject (err);
          let items = data.Items.map (item => {
            let Struct = types [item.type];
            return new Struct (item, true)
          });
          resolve (items);
        });
      });
    }
    // if object defined, create query
    return new Promise ((resolve, reject) => {
      if (this.type) obj.type = this.type;
      let params = {TableName};
      params.FilterExpression = Object.keys (obj).map (k => `#${k} = :${k}`).join (' AND ');
      params.ExpressionAttributeNames = Object.keys (obj).reduce ((acc, val) => {
        return {...acc, [('#' + val)]: val}
      }, {});
      params.ExpressionAttributeValues = Object.keys (obj).reduce ((acc, val) => {
        return {...acc, [(':' + val)]: obj [val]}
      }, {});
      docClient.scan (params, (err, data) => {
        if (err) return reject (err);
        let items = data.Items.map (item => {
          let Struct = types [item.type];
          return new Struct (item, true)
        });
        resolve (items);
      })
    });
  }
  // populate a list
  static async populateAll (list) {
    return new Promise (async (resolve, reject) => {
      try {
        let db = await Model.find ();
          list.forEach (item => {
            item.refs.forEach (ref => {
              if (item.obj [ref]) {
                item.populated [ref] = db.filter (record => item.obj [ref] === record.id) [0];
              }
            })
          });
        resolve ();
      } catch (e) {
        reject (e);
      }
    });
  }
  // populate based on provided refs
  async populate () {
    return new Promise (async (resolve, reject) => {
      try {
        await Promise.all (this.refs.map (ref => new Promise (async (resolve, reject) => {
          try {
            if (this.obj [ref]) this.populated [ref] = (await Model.find ({id: this.obj [ref]})) [0];
            resolve ();
          } catch (e) {
            reject (e);
          }
        })));
        resolve (this);
      } catch (e) {
        reject (e);
      }
    });
  }
  toJSON () {
    return Object.assign ({...this.obj, id: this.id, type: this.type, modifiedAt: this.modifiedAt, createdAt: this.createdAt}, this.populated);
  }
  toString () {
    return this.id;
  }
}

module.exports = Model;
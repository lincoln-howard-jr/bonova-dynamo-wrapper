const schm = require ('schm');
const _id = require ('uuid/v4');
const AWS = require ('aws-sdk');
const docClient = new AWS.DynamoDB.DocumentClient ();
const TableName = process.env.DB;

// root Model class (abstract class)
class Model {
  // constructor takes a a schema and type name
  // creates methods necessary for all models
  constructor (schema=schm ({}), type, obj) {
    this.schema = schema;
    this.type = type;
    this.obj = obj;
    this.id = _id ();
    this.createdAt = new Date ().toString ();
    this.modifiedAt = new Date ().toString ();
    this.created = false;
    this.lock = false;
  }
  // async write to db
  async save () {
    // if locked, stop execution, else lock
    if (this.lock) return new Promise (resolve => resolve ([new Error ('locked'), null]));
    this.lock = true;
    // if the record exists in the db, update by id
    if (this.created) {
      return new Promise (async (resolve) => {
        try {
          await this.schema.validate (this.obj);
          // set up params
          let params = {TableName};
          params.Key = {id: this.id};
          params.UpdateExpression = Object.keys (this.schema.params).map (k => `set #${k} = :${k}`).join (', ');
          params.UpdateExpression += ', set #modifiedAt = :modifiedAt';
          params.ExpressionAttributeNames = Object.keys (this.schema.params).reduce ((acc, val) => {
            return {...acc, [('#' + val)]: val};
          }, {'#modifiedAt': 'modifiedAt'});
          params.ExpressionAttributeValues = Object.keys (this.schema.params).reduce ((acc, val) => {
            return {...acc, [(':' + val)]: this.obj [val]};
          }, {modifiedAt: new Date ()});
          docClient.update (params, (err, data) => {
            this.lock = false;
            resolve ([err, data]);
          })
        } catch (e) {
          resolve ([e, null]);
        }
      });
    }
    // first save - create the item in the table
    return new Promise (async (resolve) => {
      try {
        // ensure type
        await this.schema.validate (this.obj);
        let Item = {...this.obj, type: this.type, id: this.id, createdAt: this.createdAt, modifiedAt: this.modifiedAt};
        docClient.put ({TableName, Item}, (err, data) => {
          if (err) resolve ([err, null]);
          this.created = true;
          this.lock = false;
          resolve ([null, data]);
        })
      } catch (e) {
        resolve ([e, null]);
      }
    });
  }
  // find (all) method
  static find (obj) {
    // if nothing is specified, return every document
    if (!obj) {
      return new Promise (resolve => {
        docClient.scan ({TableName}, (err, data) => {
          if (err) return resolve ([err, null]);
          resolve ([null, data.Items]);
        });
      });
    }
    // if object defined, create query
    return new Promise (resolve => {
      let params = {TableName};
      params.FilterExpression = Object.keys (obj).map (k => `#${k} = :${k}`).join (', ');
      params.ExpressionAttributeNames = Object.keys (obj).reduce ((acc, val) => {
        return {...acc, [('#' + val)]: val}
      }, {});
      params.ExpressionAttributeValues = Object.keys (obj).reduce ((acc, val) => {
        return {...acc, [(':' + val)]: obj [val]}
      }, {});
      docClient.scan (params, (err, data) => {
        if (err) return resolve ([err, null]);
        resolve ([null, data.Items]);
      })
    });
  }
  toJSON () {
    return this.obj;
  }
}

module.exports = Model;
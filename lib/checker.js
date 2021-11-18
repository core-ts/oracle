"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var OracleChecker = (function () {
  function OracleChecker(client, service, timeout) {
    this.client = client;
    this.timeout = (timeout ? timeout : 4200);
    this.service = (service ? service : 'oracle');
    this.check = this.check.bind(this);
    this.name = this.name.bind(this);
    this.build = this.build.bind(this);
  }
  OracleChecker.prototype.check = function () {
    var _this = this;
    var obj = {};
    var promise = new Promise(function (resolve, reject) {
      _this.client.ping(function (err) {
        if (err) {
          return reject(err);
        }
        else {
          resolve(obj);
        }
      });
    });
    if (this.timeout > 0) {
      return promiseTimeOut(this.timeout, promise);
    }
    else {
      return promise;
    }
  };
  OracleChecker.prototype.name = function () {
    return this.service;
  };
  OracleChecker.prototype.build = function (data, err) {
    if (err) {
      if (!data) {
        data = {};
      }
      data['error'] = err;
    }
    return data;
  };
  return OracleChecker;
}());
exports.OracleChecker = OracleChecker;
function promiseTimeOut(timeoutInMilliseconds, promise) {
  return Promise.race([
    promise,
    new Promise(function (resolve, reject) {
      setTimeout(function () {
        reject("Timed out in: " + timeoutInMilliseconds + " milliseconds!");
      }, timeoutInMilliseconds);
    })
  ]);
}

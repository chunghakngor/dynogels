import async from "async";
import * as _ from "lodash";

const internals: any = {};

internals.createTable = (
  model: any,
  globalOptions: any,
  options: any,
  callback: (error?: any) => void
) => {
  globalOptions = globalOptions || {};
  options = options || {};

  const tableName = model.tableName();

  model.describeTable((err: any, data: any) => {
    if (_.isNull(data) || _.isUndefined(data)) {
      model.log.info("creating table: %s", tableName);
      return model.createTable(options, (error: any) => {
        if (error) {
          model.log.warn(
            { err: error },
            "failed to create table %s: %s",
            tableName,
            error
          );
          return callback(error);
        }

        model.log.info("waiting for table: %s to become ACTIVE", tableName);
        internals.waitTillActive(globalOptions, model, callback);
      });
    } else {
      model.updateTable((err: any) => {
        if (err) {
          model.log.warn(
            { err: err },
            "failed to update table %s: %s",
            tableName,
            err
          );
          return callback(err);
        }

        model.log.info("waiting for table: %s to become ACTIVE", tableName);
        internals.waitTillActive(globalOptions, model, callback);
      });
    }
  });
};

internals.waitTillActive = (
  options: any,
  model: any,
  callback: (error?: any) => void
) => {
  let status = "PENDING";

  async.doWhilst(
    (callback: (error?: any) => void) => {
      model.describeTable((err: any, data: any) => {
        if (err) {
          return callback(err);
        }

        status = data.Table.TableStatus;

        setTimeout(callback, options.pollingInterval || 1000);
      });
    },
    () => status !== "ACTIVE",
    (err: any) => callback(err)
  );
};

export default (models: any, config: any, callback: (error?: any) => void) => {
  async.eachSeries(
    _.keys(models),
    (key: string, callback: (error?: any) => void) =>
      internals.createTable(
        models[key],
        config.$dynogels,
        config[key],
        callback
      ),
    callback
  );
};

const Promise = require('bluebird');

module.exports = {
  directive: 'STOR',
  handler: function ({log, command} = {}) {
    if (!this.fs) return this.reply(550, 'File system not instantiated');
    if (!this.fs.write) return this.reply(402, 'Not supported by file system');

    const append = command.directive === 'APPE';
    const fileName = command.arg;

    log.trace({ connectionId: this.id }, "starting promise chain with waitForConnection");
    return this.connector.waitForConnection()
    .tap(() => this.commandSocket.pause())
    .then(() => Promise.try(() => this.fs.write(fileName, {append, start: this.restByteCount})))
    .then((fsResponse) => {
      log.trace({ connectionId: this.id }, "fsResponse");

      let {stream, clientPath} = fsResponse;
      if (!stream && !clientPath) {
        stream = fsResponse;
        clientPath = fileName;
      }
      const serverPath = stream.path || fileName;

      const destroyConnection = (connection, reject) => (err) => {
        log.trace({ connectionId: this.id, err }, "attempting to destroy connection");

        try {
          if (connection) {
            if (connection.writable) {
              log.trace({ connectionId: this.id }, "calling connection.end");
              connection.end();
            }

            log.trace({ connectionId: this.id  }, "calling connection.destroy");
            connection.destroy(err);
          }
        } finally {
          log.trace({ connectionId: this.id }, "calling destroy connection rejection callback");
          reject(err);
        }
      };

      const streamPromise = new Promise((resolve, reject) => {
        const destroySocketDelegate = destroyConnection(this.connector.socket, reject);
        stream.once('error', (err) => {
          log.trace({ connectionId: this.id, err }, "stream.once('error')");
          destroySocketDelegate();
        });
        stream.once('finish', () => {
          log.trace({ connectionId: this.id }, "stream.once('finish'); resolving stream promise");
          resolve();
        });
      });

      const socketPromise = new Promise((resolve, reject) => {
        this.connector.socket.on('data', (data) => {
          log.trace({ connectionId: this.id, length: data.length }, "socket.on('data')");

          if (this.connector.socket) {
            this.connector.socket.pause();
          }
          if (stream && stream.writable) {
            log.trace({ connectionId: this.id, length: data.length }, "writing data to filesystem stream");

            stream.write(data, () => {
              log.trace({ connectionId: this.id }, "write complete");
              if(this.connector.socket)
              {
                log.trace({ connectionId: this.id }, "resuming socket");
                this.connector.socket.resume();
              }
            });
          }
        });

        this.connector.socket.once('end', () => {
          log.trace({ connectionId: this.id }, "socket.on('end')");

          if (stream.listenerCount('close')) {
            log.trace({ connectionId: this.id }, "calling stream.emit('close')");
            stream.emit('close');
          } else {
            log.trace({ connectionId: this.id }, "calling stream.end");
            stream.end();
          }

          log.trace({ connectionId: this.id }, "resolving socket promise");
          resolve();
        });
        this.connector.socket.once('error', destroyConnection(stream, reject));
      });

      this.restByteCount = 0;

      return this.reply(150).then(() => {
        if(this.connector.socket) {
          log.trace({ connectionId: this.id }, "resuming socket after 150 reply");
          return this.connector.socket.resume();
        }
      })
      .then(() => Promise.all([streamPromise, socketPromise]))
      .tap(() => {
        log.trace({ connectionId: this.id }, "emitting 'STOR' event");
        this.emit('STOR', null, serverPath);
      })
      .then(() => {
        log.trace({ connectionId: this.id }, "sending 226 reply");
        this.reply(226, clientPath);
      })
      .finally(() => {
        log.trace({ connectionId: this.id }, "promise chain complete; destroying stream");
        if(stream.destroy) {
          stream.destroy();
        }
      });
    })
    .catch(Promise.TimeoutError, (err) => {
      log.error(err);
      return this.reply(425, 'No connection established');
    })
    .catch((err) => {
      log.error(err);
      this.emit('STOR', err);
      return this.reply(550, err.message);
    })
    .finally(() => {
      this.connector.end();
      this.commandSocket.resume();
    });
  },
  syntax: '{{cmd}} <path>',
  description: 'Store data as a file at the server site'
};

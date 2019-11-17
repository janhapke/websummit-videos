const Stream = require('stream');

module.exports = class PersistStream extends Stream.Writable {

    constructor(db) {
        super({objectMode: true});
        this.db = db;
    }

    setup() {
        return new Promise((resolve, reject) => {
            this.db.serialize(() => {
                this.db.run('DROP TABLE IF EXISTS talks');
                this.db.run(`
                    CREATE TABLE talks (
                        id VARCHAR(36),
                        stage VARCHAR(255),
                        title VARCHAR(1024),
                        description TEXT,
                        date VARCHAR(255),
                        start_time VARCHAR(255),
                        end_time VARCHAR(255),
                        duration_sec INT,
                        topics JSON,
                        presenters JSON
                    )
                `, () => {
                    resolve();
                });
            });
        });
    }    

    _write(chunk, encoding, callback) {
        this.db.run(
            `
                INSERT INTO talks
                (id, stage, title, description, date, start_time, end_time, duration_sec, topics, presenters)
                VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `
            ,
            [
                chunk.id,
                chunk.stage,
                chunk.title,
                chunk.description,
                chunk.date,
                chunk.start_time,
                chunk.end_time,
                chunk.duration_sec,
                JSON.stringify(chunk.topics),
                JSON.stringify(chunk.presenters),
            ],
            (error) => {
                callback(error);
            }
        );
    }

};

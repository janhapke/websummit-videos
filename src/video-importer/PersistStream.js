const Stream = require('stream');

module.exports = class PersistStream extends Stream.Writable {

    constructor(db) {
        super({objectMode: true});
        this.db = db;
    }

    setup() {
        return new Promise((resolve, reject) => {
            this.db.serialize(() => {
                this.db.run('DROP TABLE IF EXISTS videos');
                this.db.run(`
                    CREATE TABLE videos (
                        uri VARCHAR(255),
                        name VARCHAR(1024),
                        description TEXT,
                        link VARCHAR(255),
                        duration_sec INT,
                        release_time DATETIME,
                        pic_small VARCHAR(255),
                        pic_medium VARCHAR(255),
                        pic_large VARCHAR(255)
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
                INSERT INTO videos
                (uri, name, description, link, duration_sec, release_time, pic_small, pic_medium, pic_large)
                VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?)
            `
            ,
            [
                chunk.uri,
                chunk.name,
                chunk.description,
                chunk.link,
                chunk.duration_sec,
                chunk.release_time,
                chunk.pic_small,
                chunk.pic_medium,
                chunk.pic_large,
            ],
            (error) => {
                callback(error);
            }
        );
    }

};

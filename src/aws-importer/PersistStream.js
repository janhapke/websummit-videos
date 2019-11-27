const Stream = require('stream');

module.exports = class PersistStream extends Stream.Writable {

    constructor(db) {
        super({objectMode: true});
        this.db = db;
    }

    setup() {
        return new Promise((resolve, reject) => {
            this.db.serialize(() => {
                this.db.run('DROP TABLE IF EXISTS aws');
                this.db.run(`
                    CREATE TABLE aws (
                        slug VARCHAR(1024),
                        title VARCHAR(1024),
                        link VARCHAR(1024),
                        image_url VARCHAR(1024)
                    )
                `, (error) => {
                    if (error) {
                        reject(error);
                    } else {
                        resolve();
                    }
                });
            });
        });
    }    

    _write(chunk, encoding, callback) {
        this.db.run(
            `
                INSERT INTO aws
                (slug, title, link, image_url)
                VALUES
                (?, ?, ?, ?)
            `
            ,
            [
                chunk.slug,
                chunk.title,
                chunk.link,
                chunk.image_url,
            ],
            (error) => {
                callback(error);
            }
        );
    }

};

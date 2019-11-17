const Vimeo = require('vimeo').Vimeo;

const VimeoStream = require('./VimeoStream');
const TransformStream = require('./TransformStream');
const PersistStream = require('./PersistStream');

const config = require('../../config');

process.on('SIGINT', () => {
    console.log('exiting on SIGINT');
    process.exit();
});

process.on('SIGTERM', () => {
    console.log('exiting on SIGTERM');
    process.exit();
});

const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database(config.database.filename);

const client = new Vimeo(config.vimeo.client_id, config.vimeo.client_secret, config.vimeo.access_token);

const videoStream = new VimeoStream(client, `/users/${config.vimeo.user_id}/videos?per_page=${config.vimeo.per_page}`);
const transformStream = new TransformStream();
const persistStream = new PersistStream(db);

persistStream.setup().then(() => {
    videoStream.pipe(transformStream).pipe(persistStream);
});

persistStream.on('end', () => {
    console.log('persistStream.end');
    db.close();
})

// db.close();

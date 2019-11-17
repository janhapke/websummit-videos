const Stream = require('stream');

const ScheduleStream = require('./ScheduleStream');
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


const logStream = new Stream.Writable({
    objectMode: true,
    write(chunk, encoding, callback) {
        console.log(chunk);
        callback();
    }
});

const scheduleStream = new ScheduleStream();
const transformStream = new TransformStream();
const persistStream = new PersistStream(db);

persistStream.setup().then(() => {
    scheduleStream.pipe(transformStream).pipe(persistStream);
})

scheduleStream.on('error', error => {
    console.error('scheduleStream.error', error);
});

const fs = require('fs');
const Handlebars = require('handlebars');

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

function loadTalkIds() {
    return new Promise((resolve, reject) => {
        db.all('SELECT DISTINCT id FROM talks', [], (error, rows) => {
            if (error) {
                return reject(error);
            }
            const talks = rows.map(row => row.id);
            resolve(talks);
        });
    });
};

function loadTalkDetails(talkId) {
    return new Promise((resolve, reject) => {
        db.all('SELECT * FROM talks WHERE id = ? LIMIT 1', [talkId], (error, rows) => {
            if (error) {
                return reject(error);
            }
            resolve(rows[0]);
        });
    });
};

function loadMatchingVideos(talkId) {
    return new Promise((resolve, reject) => {
        db.all('SELECT * FROM matches LEFT JOIN videos ON (matches.video_uri = videos.uri) WHERE matches.talk_id = ?', [talkId], (error, rows) => {
            if (error) {
                return reject(error);
            }
            resolve(rows);
        });
    });
};

loadTalkIds()
.then(ids => Promise.all(
    ids.map(
        talkId => Promise.all([
            loadTalkDetails(talkId),
            loadMatchingVideos(talkId),
        ])
    )
))
.then(talkData => talkData.map(
    data => {
        const talkDetails = data[0];
        const videos = data[1];
        return {
            id: talkDetails.id,
            title: talkDetails.title,
            stage: talkDetails.stage,
            date: talkDetails.date,
            start_time: talkDetails.start_time,
            end_time: talkDetails.end_time,
            topics: JSON.parse(talkDetails.topics),
            presenters: JSON.parse(talkDetails.presenters),
            description: talkDetails.description,
            videos: videos.map(video => {
                return {
                    name: video.name,
                    description: video.description,
                    link: video.link,
                    release_time: video.release_time,
                    pic_small: video.pic_small,
                    pic_medium: video.pic_medium,
                    pic_large: video.pic_large,
                }
            }),
        }
    })
)
.then(talks => {
    return new Promise((resolve, reject) => {
        fs.writeFile('talks.json', JSON.stringify(talks, null, 2), (err) => {
            if (err) {
                reject(err);
            }
            resolve(talks);
        });
    });
})
.then(talks => {
    fs.readFile('src/exporter/talks.hbs', 'utf8', (error, source) => {

        const template = Handlebars.compile(source);
        Handlebars.registerHelper('date', function (string) {
            return string.substr(0, 10);
        });
        Handlebars.registerHelper('sort', function (arr) {
            return arr.sort();
        });
        Handlebars.registerHelper('formatPresenter', function (p) {
            return p.name + ' (' + p.companyName + ')';
        });

        const html = template({talks: talks});

        return new Promise((resolve, reject) => {
            fs.writeFile('index.html', html, (err) => {
                if (err) {
                    reject(err);
                }
                resolve(talks);
            });
        });
    });
});

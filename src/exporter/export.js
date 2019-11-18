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

function loadUnmatchedVideos() {
    return new Promise((resolve, reject) => {
        db.all(
            `
                SELECT DISTINCT *
                FROM videos
                WHERE release_time >= '2019-11-01'
                AND uri NOT IN (SELECT DISTINCT video_uri FROM matches)
            `,
            [],
            (error, rows) => {
                if (error) {
                    return reject(error);
                }
                resolve(rows);
            }
        );
    });
}

function loadStats() {
    return new Promise((resolve, reject) => {
        db.all(
            `
                SELECT
                    CURRENT_DATE AS last_update,
                    (SELECT COUNT(*) FROM talks) AS num_talks,
                    (SELECT COUNT(DISTINCT talk_id) FROM matches) AS num_matched_talks,
                    (SELECT COUNT(*) FROM videos where release_time >= '2019-11-01') AS num_videos,
                    (SELECT COUNT(DISTINCT video_uri) FROM matches) AS num_matched_videos
            `,
            [],
            (error, rows) => {
                if (error) {
                    return reject(error);
                }
                resolve(rows);
            }
        );
    });
}

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
    console.debug('exporting sessions...');
    return new Promise((resolve, reject) => {
        fs.readFile('src/exporter/sessions.hbs', 'utf8', (error, source) => {

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

            fs.writeFile('index.html', html, (err) => {
                if (err) {
                    reject(err);
                }
                resolve(talks);
            });
        });
    });
})
.then(loadUnmatchedVideos)
.then(videos => {
    console.debug('exporting videos...');
    return new Promise((resolve, reject) => {
        fs.readFile('src/exporter/videos.hbs', 'utf8', (error, source) => {

            const template = Handlebars.compile(source);
            Handlebars.registerHelper('nl2br', function (string) {
                return (string || '').replace("\n", "<br />\n");
            });

            const html = template({videos: videos});

            fs.writeFile('videos.html', html, (err) => {
                if (err) {
                    reject(err);
                }
                resolve(videos);
            });
        });
    });
})
.then(loadStats)
.then(stats => {
    console.debug('exporting about...');
    return new Promise((resolve, reject) => {
        fs.readFile('src/exporter/about.hbs', 'utf8', (error, source) => {

            const template = Handlebars.compile(source);

            const html = template({stats: stats[0]});

            fs.writeFile('about.html', html, (err) => {
                if (err) {
                    reject(err);
                }
                resolve(stats);
            });
        });
    });
})
;

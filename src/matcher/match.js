const slugify = require('slugify');

const config = require('../../config');

process.on('SIGINT', () => {
    console.log('exiting on SIGINT');
    process.exit();
});

process.on('SIGTERM', () => {
    console.log('exiting on SIGTERM');
    process.exit();
});

function slugifyString(string) {
    return slugify(string.replace('Q+A', 'Q&A').toLowerCase(), {remove: /[*,\?´+~.()'"!:@]/g}).trim();
}

const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database(config.database.filename);

function setupDatabase() {
    return new Promise((resolve, reject) => {
        db.serialize(() => {
            db.run('DROP TABLE IF EXISTS matches');
            db.run(`
                CREATE TABLE matches (
                    talk_id VARCHAR(36),
                    video_uri VARCHAR(255)
                )
            `, () => {
                resolve();
            });
        });
    });
};

function loadTalks() {
    return new Promise((resolve, reject) => {
        db.all('SELECT DISTINCT id, stage, title, presenters FROM talks', [], (error, rows) => {
            if (error) {
                return reject(error);
            }
            const talks = rows.map(row => {
                return {
                    id: row.id,
                    stage: row.stage,
                    title: row.title,
                    title_slug: slugifyString(row.title),
                    presenters: row.presenters,
                };
            });
            resolve(talks);
        });
    });
}

function loadVideos() {
    return new Promise((resolve, reject) => {
        db.all('SELECT DISTINCT uri, name, description, release_time FROM videos where release_time >= "2019-11-01"', [], (error, rows) => {
            if (error) {
                return reject(error);
            }
            const videos = rows.map(row => {
                return {
                    uri: row.uri,
                    name: row.name,
                    name_slug: slugifyString(row.name),
                    description: row.description,
                    release_time: row.release_time,
                };
            });
            resolve(videos);
        });
    });
}

setupDatabase()
.then(() => Promise.all([loadTalks(), loadVideos()]))
.then(data => {
    return {
        allTalks: data[0],
        allVideos: data[1],
        unmatchedTalks: data[0],
        unmatchedVideos: data[1],
        matches: [],
    }
})
.then(result => {
    const manualMatches = [
        { talk_id: '62e2e333-7dd6-45e3-9fda-60e06af8d01b', video_uri: '/videos/370976186'},
        { talk_id: '8ab58a18-3d4c-4d2b-8a8b-fe2983ab6113', video_uri: '/videos/371722923'},
        { talk_id: '0fb8a55b-7f8f-45a2-91ef-d82a226dcad6', video_uri: '/videos/371532933'},
        { talk_id: '0235c182-6a0f-440a-bbe7-f9aab349aa69', video_uri: '/videos/371369579'},
        { talk_id: '400a7bd5-bc1e-425b-a395-e77e92d42878', video_uri: '/videos/371339532'},
    ];
    const matchedTalkIds = [];
    manualMatches.forEach(match => {
        const matchedTalks = result.unmatchedTalks.filter(talk => talk.id == match.talk_id);
        if (matchedTalks.length != 1) {
            console.error('manual match did not find 1 talk', match, matchedTalks);
            return;
        }
        const talk = matchedTalks[0];
        const matchedVideos = result.unmatchedVideos.filter(video => video.uri == match.video_uri);
        if (matchedVideos.length != 1) {
            return;
        }
        const matchingVideo = matchedVideos[0];
        result.matches.push({
            talk_id: talk.id,
            video_uri: matchingVideo.uri
        });
        matchedTalkIds.push(talk.id);
        result.unmatchedVideos = result.unmatchedVideos.filter(video => video.uri != matchingVideo.uri);
    });
    result.unmatchedTalks = result.unmatchedTalks.filter(talk => matchedTalkIds.indexOf(talk.id) === -1);
    console.log('match manually');
    console.log('matched talks (this pass)', matchedTalkIds.length);
    console.log('unmatched talks', result.unmatchedTalks.length/*, result.unmatchedTalks */);
    console.log('unmatched videos', result.unmatchedVideos.length);
    return result;
})
.then(result => {
    const matchedTalkIds = [];
    result.unmatchedTalks.forEach(talk => {
        const matchingVideos = result.unmatchedVideos.filter(video => video.name_slug == talk.title_slug);
        if (matchingVideos.length == 1) {
            const matchingVideo = matchingVideos[0];
            result.matches.push({
                talk_id: talk.id,
                video_uri: matchingVideo.uri
            });
            matchedTalkIds.push(talk.id);
            result.unmatchedVideos = result.unmatchedVideos.filter(video => video.uri != matchingVideo.uri);
        }
    });
    result.unmatchedTalks = result.unmatchedTalks.filter(talk => matchedTalkIds.indexOf(talk.id) === -1);
    console.log('match title exactly');
    console.log('matched talks (this pass)', matchedTalkIds.length);
    console.log('unmatched talks', result.unmatchedTalks.length/*, result.unmatchedTalks */);
    console.log('unmatched videos', result.unmatchedVideos.length);
    return result;
})
.then(result => {
    const matchedTalkIds = [];
    result.unmatchedTalks.forEach(talk => {
        const matchingVideos = result.unmatchedVideos.filter(video => video.name_slug == talk.title_slug);
        if (matchingVideos.length <= 1) {
            // can only process multiple matches
            return;
        }
        const presenters = JSON.parse(talk.presenters);
        if (presenters.length !== 1) {
            // can only process single presenters
            return;
        }

        const secondPassMatchingVideos = matchingVideos.filter(video => slugifyString((video.description || '').split('\n')[0]) == slugifyString(presenters.map(presenter => presenter.name).join(' ')));
        if (secondPassMatchingVideos.length !== 1) {
            // no match or multiple matches
            return;
        }

        const matchingVideo = secondPassMatchingVideos[0];
        result.matches.push({
            talk_id: talk.id,
            video_uri: matchingVideo.uri
        });
        matchedTalkIds.push(talk.id);
        result.unmatchedVideos = result.unmatchedVideos.filter(video => video.uri != matchingVideo.uri);
    });
    result.unmatchedTalks = result.unmatchedTalks.filter(talk => matchedTalkIds.indexOf(talk.id) === -1);
    console.log('match via presenter');
    console.log('matched talks (this pass)', matchedTalkIds.length);
    console.log('unmatched talks', result.unmatchedTalks.length); //, result.unmatchedTalks);
    console.log('unmatched videos', result.unmatchedVideos.length);
    console.log(result.unmatchedVideos);
    return result;
})
.then(result => {
    const matchedTalkIds = [];
    result.unmatchedTalks.forEach(talk => {
        const matchingVideos = result.unmatchedVideos
            .filter(video => video.name_slug != 'opening-remarks')
            .filter(video => video.name_slug == talk.title_slug);

        if (matchingVideos.length <= 1) {
            // only match multiple videos
            return
        }

        matchingVideos.forEach(matchingVideo => {
            result.matches.push({
                talk_id: talk.id,
                video_uri: matchingVideo.uri
            });
            result.unmatchedVideos = result.unmatchedVideos.filter(video => video.uri != matchingVideo.uri);
        })
        matchedTalkIds.push(talk.id);

    });
    result.unmatchedTalks = result.unmatchedTalks.filter(talk => matchedTalkIds.indexOf(talk.id) === -1);
    console.log('match multiple');
    console.log('matched talks (this pass)', matchedTalkIds.length);
    console.log('unmatched talks', result.unmatchedTalks.length/*, result.unmatchedTalks */);
    console.log('unmatched videos', result.unmatchedVideos.length);
    return result;
})
.then(result => {
    result.matches.forEach(match => {
        db.run(
            `
                INSERT INTO matches
                (talk_id, video_uri)
                VALUES
                (?, ?)
            `
            ,
            [
                match.talk_id,
                match.video_uri
            ],
            (error) => {
                if (error) {
                    throw error;
                }
            }
        );
    })
});

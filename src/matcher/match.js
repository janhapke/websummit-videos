const slugify = require('slugify');
const { DateTime } = require('luxon');
const editDistance = require("edit-distance")

function calculateEditDistance(stringA, stringB) {
    const lev = editDistance.levenshtein(stringB, stringA, () => 1, () => 1, (a,b) => a !== b ? 1 : 0);
    return lev.distance
}

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
    return slugify(string.replace('Q+A', 'Q&A').toLowerCase(), {remove: /[*,\/\?´+~.()'"!:@]/g}).trim();
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
            `);
            db.run('DROP TABLE IF EXISTS slide_matches');
            db.run(`
                CREATE TABLE slide_matches (
                    talk_id VARCHAR(36),
                    slide_slug VARCHAR(255)
                )
            `, (error) => {
                if (error) {
                    return reject(error);
                }
                resolve();
            });
        });
    });
};

function loadTalks() {
    return new Promise((resolve, reject) => {
        db.all('SELECT DISTINCT id, stage, title, description, presenters, start_time, end_time FROM talks', [], (error, rows) => {
            if (error) {
                return reject(error);
            }
            const talks = rows.map(row => {
                return {
                    id: row.id,
                    stage: row.stage,
                    title: row.title,
                    title_slug: slugifyString(row.title),
                    description: row.description,
                    presenters: row.presenters,
                    start_time: row.start_time,
                    end_time: row.end_time,
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

function loadAws() {
    return new Promise((resolve, reject) => {
        db.all('SELECT DISTINCT slug, title, link, image_url FROM aws', [], (error, rows) => {
            if (error) {
                return reject(error);
            }
            const aws = rows.map(row => {
                return {
                    slug: row.slug,
                    title: row.title,
                    link: row.link,
                    image_url: row.image_url
                };
            });
            resolve(aws);
        });
    });
}

setupDatabase()
.then(() => Promise.all([loadTalks(), loadVideos(), loadAws()]))
.then(data => {
    return {
        allTalks: data[0],
        allVideos: data[1],
        allSlides: data[2],
        unmatchedTalks: data[0],
        unmatchedVideos: data[1],
        unmatchedSlides: data[2],
        matches: [],
        slideMatches: [],
    }
})
.then(result => {
    const matchedSlideSlugs = [];
    let awsSessions = result.unmatchedTalks.filter(talk => talk.stage == 'AWS Developer Workshops');
    result.unmatchedSlides.forEach(slide => {
        const exactMatches = awsSessions.filter(talk => talk.title_slug == slide.slug)
        exactMatches.forEach(matchingTalk => {
            result.slideMatches.push({
                talk_id: matchingTalk.id,
                talk_title_slug: matchingTalk.title_slug,
                slide_slug: slide.slug
            });
            awsSessions = awsSessions.filter(talk => talk.id != matchingTalk.id);
        });
        if (exactMatches.length > 0) {
            matchedSlideSlugs.push(slide.slug);
        }
    });

    console.debug('matched exactly', matchedSlideSlugs.length);
    console.debug('unmatched sessions', awsSessions.length);
    console.debug('unmatched slides', result.unmatchedSlides.length);

    let distances = result.unmatchedSlides.map(slide =>
        awsSessions.map(talk => {
            return {
                talk_title_slug: talk.title_slug,
                slide_slug: slide.slug,
                distance: calculateEditDistance(talk.title_slug, slide.slug)
            }
        })
    ).reduce((a, b) => a.concat(b), []).sort((a, b) => a.distance - b.distance);
    const maxIterations = distances.length;
    for (let i = 0; i < maxIterations && distances.length > 0; ++i) {
        const distance = distances[0];
        const matches = awsSessions.filter(talk => talk.title_slug == distance.talk_title_slug);
        matches.forEach(match => {
            result.slideMatches.push({
                talk_id: match.id,
                talk_title_slug: distance.talk_title_slug,
                slide_slug: distance.slide_slug
            });
        });
        awsSessions = awsSessions.filter(talk => talk.title_slug != distance.talk_title_slug);
        distances = distances.filter(cur => cur.talk_title_slug !== distance.talk_title_slug && cur.slide_slug !== distance.slide_slug).sort((a, b) => a.distance - b.distance);
        matchedSlideSlugs.push(distance.slide_slug);
    }
    result.unmatchedSlides = result.unmatchedSlides.filter(slide => matchedSlideSlugs.indexOf(slide.slug) == -1);
    // const distances = result.unmatchedSlides.map(slide => awsSessions.map(talk => { return { talk: talk.title_slug, slide: slide.slug, distance: calculateEditDistance(talk.title_slug, slide.slug) } }));
    // console.debug(distances);
    // throw 'Whatever!';
    return result;
})
.then(result => {
    const manualMatches = [
        { talk_id: '62e2e333-7dd6-45e3-9fda-60e06af8d01b', video_uri: '/videos/370976186'},
        { talk_id: '8ab58a18-3d4c-4d2b-8a8b-fe2983ab6113', video_uri: '/videos/371722923'},
        { talk_id: '0fb8a55b-7f8f-45a2-91ef-d82a226dcad6', video_uri: '/videos/371532933'},
        { talk_id: '0235c182-6a0f-440a-bbe7-f9aab349aa69', video_uri: '/videos/371369579'},
        { talk_id: '400a7bd5-bc1e-425b-a395-e77e92d42878', video_uri: '/videos/371339532'},
        { talk_id: 'a7d9e021-9883-4e4e-ae22-a38b51b80d16', video_uri: '/videos/373891121'},
        { talk_id: '26aadd95-c7d6-40b3-be13-2715f2834038', video_uri: '/videos/371897900'},
        { talk_id: 'dd835a59-1ee3-4de2-9e48-3d8d3ca61e01', video_uri: '/videos/371335329'},
        { talk_id: '7c7842c9-3e52-4ef8-bfde-6c626eee847e', video_uri: '/videos/371288305'},
        { talk_id: '8cb35e11-4183-4853-9d46-c1dca65a3b36', video_uri: '/videos/371209481'},
        { talk_id: '6f21a5ac-a4d9-4be1-8598-b82290dbafdd', video_uri: '/videos/371178506'},
        { talk_id: '4a29cc9a-086f-41c4-9e76-896372714897', video_uri: '/videos/371156854'},
        { talk_id: '0708fd2b-82b4-403f-84b0-8bacd6d1cf2e', video_uri: '/videos/371134022'},
        { talk_id: '713e026f-aaed-45c7-89ed-303e1d902d74', video_uri: '/videos/371131728'},
        { talk_id: 'a421dcf6-c709-47c8-9338-de768cfdb856', video_uri: '/videos/371129537'},
        { talk_id: 'd86e7814-758d-42f3-a2c6-c4ac9fe0d2c4', video_uri: '/videos/371128235'},
        { talk_id: '64093230-22f1-4f95-adc4-ccb2171d1ebe', video_uri: '/videos/371879132'},
        { talk_id: '52f8302a-b9fc-4eab-bc24-3bfcd49e430d', video_uri: '/videos/371760083'},
        { talk_id: 'e3898815-c179-4865-b49c-c5960add5cb3', video_uri: '/videos/371703970'},
        { talk_id: '3265e843-4d4c-4cb5-9d4f-a7379991b0db', video_uri: '/videos/371671141'},
        { talk_id: '4c0432e1-70f0-4142-9405-efa53da6f706', video_uri: '/videos/371648445'},
        { talk_id: '9cc0570c-29d4-4b56-8620-8432dfc0cd5d', video_uri: '/videos/371578468'},
        { talk_id: '9cc0570c-29d4-4b56-8620-8432dfc0cd5d', video_uri: '/videos/371164541'},
        { talk_id: '9d714d63-b884-4c31-aee7-36124f71ac27', video_uri: '/videos/371402653'},
        { talk_id: 'a0ce662b-cb37-41b9-9e74-9e0b781f25bc', video_uri: '/videos/371378992'},
        { talk_id: '4652c0b0-9b47-49fd-b0a6-fc1b237b7b09', video_uri: '/videos/371360451'},
        { talk_id: '4652c0b0-9b47-49fd-b0a6-fc1b237b7b09', video_uri: '/videos/371159432'},
        { talk_id: 'a31a5083-bf9c-4ad9-a8ea-fe20e825f116', video_uri: '/videos/371111134'},
        { talk_id: '5b65234e-3e3a-41f0-afa8-337f56ecef08', video_uri: '/videos/371356964'},
        { talk_id: 'ed708ac7-e837-47f1-bb25-7bc45a81e4ad', video_uri: '/videos/371356808'},
        { talk_id: 'ed708ac7-e837-47f1-bb25-7bc45a81e4ad', video_uri: '/videos/371348950'},
        { talk_id: '812f199f-9fc9-4214-9191-5e4a8a78d4df', video_uri: '/videos/371278301'},
        { talk_id: 'ea893807-3848-4738-aa69-d1cc9b73e190', video_uri: '/videos/371352298'},
        { talk_id: '457f714b-695c-43f3-8226-29f0902597d2', video_uri: '/videos/371349291'},
        { talk_id: 'f8285afa-6bd0-44af-8054-00b419dbdaf9', video_uri: '/videos/371175475'},
        { talk_id: 'd02fc0ee-1086-409c-8759-6f28e208c52a', video_uri: '/videos/371120734'},
        { talk_id: '0bf64b65-4621-468c-a8ea-741ffab94a56', video_uri: '/videos/371107036'},
        { talk_id: '83e3191a-8cc1-4794-82e2-7d7430575cc9', video_uri: '/videos/370964791'},
        { talk_id: '83e3191a-8cc1-4794-82e2-7d7430575cc9', video_uri: '/videos/371347833'},
        { talk_id: '0e597982-da41-4399-b130-a7ce488655ba', video_uri: '/videos/371850278'},
        { talk_id: '25040c2a-0eab-45a3-a291-341c4bc5d09a', video_uri: '/videos/371200543'},
        { talk_id: 'd514f782-9271-46f4-b677-86c60e79a5c2', video_uri: '/videos/371394864'},
        { talk_id: 'ea03f8f8-84e1-47a9-afee-1829384c3b79', video_uri: '/videos/371629321'},
        { talk_id: '4930ed09-80e7-4ba5-8d3d-ddf2e9d7f812', video_uri: '/videos/374927089' },
        { talk_id: '4052c0c2-50ec-4875-ac66-cf28b2014572', video_uri: '/videos/371541141'},
        { talk_id: '7f1dd639-8f39-470b-bbeb-b20c8f9f7106', video_uri: '/videos/374920855'},
        { talk_id: '16886558-af9f-4e02-9596-3585f14d0455', video_uri: '/videos/371399149'},
        { talk_id: '0cf0a640-509c-4e4c-8645-36186884d20e', video_uri: '/videos/374889770'},
        { talk_id: 'aa094c33-196d-477b-a283-f05025de2dfb', video_uri: '/videos/371691177'},
        { talk_id: '88d43ad8-c396-434a-9b4f-ecea0ab0ea09', video_uri: '/videos/370981739'},
        { talk_id: 'e059363d-1a02-4c6e-9503-1de3712c9787', video_uri: '/videos/374175343'},
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
            console.error('manual match did not find 1 video', match, matchedVideos);
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
    // leave the manually matched videos in, because some of them will have duplicate matches later on
    // result.unmatchedTalks = result.unmatchedTalks.filter(talk => matchedTalkIds.indexOf(talk.id) === -1);
    console.log('match manually');
    console.log('matched talks (this pass)', matchedTalkIds.length);
    console.log('unmatched talks', result.unmatchedTalks.length/*, result.unmatchedTalks */);
    console.log('unmatched videos', result.unmatchedVideos.length);
    console.log('======================');
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
    console.log('======================');
    return result;
})
.then(result => {
    const matchedTalkIds = [];
    result.unmatchedTalks.forEach(talk => {
        if (talk.title_slug.indexOf('startup-showcase') == -1) {
            return;
        }
        const title = slugifyString(talk.stage).replace(/(startup-showcase)-(\d)/i, '$1-stage-$2');
        const startTime = DateTime.fromFormat(talk.start_time, 'HH:mm:ss').toFormat('hmm(a)?').toLowerCase();
        const endTime = DateTime.fromFormat(talk.end_time, 'HH:mm:ss').toFormat('hmma').toLowerCase();
        const searchExp = new RegExp(title + '-' + startTime + '-' + endTime);
        const matchingVideos = result.unmatchedVideos.filter(video => video.name_slug.toLowerCase().indexOf('startup-showcase') !== -1).filter(video => video.name_slug.match(searchExp) != null);
        if (matchingVideos.length == 0) {
            return;
        }
        if (matchingVideos.length == 1) {
            const matchingVideo = matchingVideos[0];
            result.matches.push({
                talk_id: talk.id,
                video_uri: matchingVideo.uri
            });
            matchedTalkIds.push(talk.id);
            result.unmatchedVideos = result.unmatchedVideos.filter(video => video.uri != matchingVideo.uri);
        } else {
            const theme = talk.description.match(/this hour focuses on (the )?(.*?),/i);
            if (!theme) {
                return;
            }
            const secondPassMatchingVideos = matchingVideos.filter(video => video.description.toLowerCase().match(new RegExp('theme: (the )?' + theme[2])));
            if (secondPassMatchingVideos.length == 1) {
                const matchingVideo = secondPassMatchingVideos[0];
                result.matches.push({
                    talk_id: talk.id,
                    video_uri: matchingVideo.uri
                });
                matchedTalkIds.push(talk.id);
                result.unmatchedVideos = result.unmatchedVideos.filter(video => video.uri != matchingVideo.uri);
            }
        }
    });
    result.unmatchedTalks = result.unmatchedTalks.filter(talk => matchedTalkIds.indexOf(talk.id) === -1);
    console.log('match time slot');
    console.log('matched talks (this pass)', matchedTalkIds.length);
    console.log('unmatched talks', result.unmatchedTalks.length/*, result.unmatchedTalks */);
    console.log('unmatched videos', result.unmatchedVideos.length);
    console.log('======================');
    return result;
})
.then(result => {
    const matchedTalkIds = [];
    result.unmatchedTalks.forEach(talk => {
        if (
            talk.title_slug.toLowerCase().indexOf('opening-remarks') == -1
            && talk.title_slug.toLowerCase().indexOf('breakout-startups') == -1
            && talk.title_slug.toLowerCase().indexOf('startup-showcase') == -1
        ) {
            return;
        }
        const matchingVideos = result.unmatchedVideos.filter(video =>
            video.name_slug.toLowerCase().indexOf('opening-remarks') !== -1
            || video.name_slug.toLowerCase().indexOf('breakout-startups') !== -1
            || video.name_slug.toLowerCase().indexOf('startup-showcase') !== -1
        );
        if (matchingVideos.length == 0) {
            return;
        }

        const presenters = JSON.parse(talk.presenters);
        if (presenters.length == 1) {
            // check if presenter appears in first line of video description
            let secondPassMatchingVideos = matchingVideos
                .filter(video => 
                    slugifyString((video.description || '').replace(/^with /i, '').split('\n')[0]) == slugifyString(presenters.map(presenter => presenter.name).join(' '))
                );
            // if presenter matches multiple, check if stage name appears at start of video name
            if (secondPassMatchingVideos.length > 1) {
                const stageSlug = slugifyString(talk.stage).toLowerCase();
                secondPassMatchingVideos = secondPassMatchingVideos
                .filter(video => 
                    video.name_slug.toLowerCase().substr(0, stageSlug.length) == stageSlug
                );
            }
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
        } else {
            // check if presenter appears in first line of video description
            const talkPresenters = presenters.map(presenter => slugifyString(presenter.name));
            let secondPassMatchingVideos = matchingVideos
                .filter(video => {
                    const videoPresenters = (video.description || '').split('\n')[0].replace(/^with /i, '').replace(/ and /i, ', ').split(/, ?/).map(name => slugifyString(name));
                    const matchingPresenters = videoPresenters.filter(videoPresenter => talkPresenters.indexOf(videoPresenter) !== -1);
                    const matchRate = matchingPresenters.length / Math.max(talkPresenters.length, videoPresenters.length);
                    return matchRate > 0.7;
                }
            );

            if (secondPassMatchingVideos.length == 1) {
                const matchingVideo = secondPassMatchingVideos[0];
                // console.log('=====> second pass match', talk, matchingVideo);
                result.matches.push({
                    talk_id: talk.id,
                    video_uri: matchingVideo.uri
                });
                matchedTalkIds.push(talk.id);
                result.unmatchedVideos = result.unmatchedVideos.filter(video => video.uri != matchingVideo.uri);
            }
        }
    });
    result.unmatchedTalks = result.unmatchedTalks.filter(talk => matchedTalkIds.indexOf(talk.id) === -1);
    console.log('match via presenter');
    console.log('matched talks (this pass)', matchedTalkIds.length);
    console.log('unmatched talks', result.unmatchedTalks.length); //, result.unmatchedTalks);
    console.log('unmatched videos', result.unmatchedVideos.length);
    console.log('======================');
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
    console.log('======================');
    return result;
})
.then(result =>
    Promise.all(
        result.matches.map(match =>
            new Promise((resolve, reject) => {
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
                            reject(error);
                        }
                        resolve();
                    }
                );
            })
        )
    ).then(() => result)
)
.then(result => 
    Promise.all(
        result.slideMatches.map(match => 
            new Promise((resolve, reject) => {
                db.run(
                    `
                        INSERT INTO slide_matches
                        (talk_id, slide_slug)
                        VALUES
                        (?, ?)
                    `
                    ,
                    [
                        match.talk_id,
                        match.slide_slug
                    ],
                    (error) => {
                        if (error) {
                            throw error;
                        }
                    }
                );
            })
        )
    ).then(() => result)
);

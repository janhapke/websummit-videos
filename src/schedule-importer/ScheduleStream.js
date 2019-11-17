const Stream = require('stream');
const request = require('request-promise');

module.exports = class ScheduleStream extends Stream.Readable {

    constructor() {
        super({objectMode: true});
        this.urls = [
            'https://api.cilabs.com/graphql?operationName=GetScheduleDay&variables=%7B%22conferenceId%22%3A%22ws19%22,%22dayId%22%3A%22MjAxOS0xMS0wNA%3D%3D%22,%22timeFormat%22%3A%22%25H%3A%25M%3A%25S%22%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1,%22sha256Hash%22%3A%22060f890092d04ad9ce812ba20b5b1d9dcc03bab04cc1a1e40257858b99082b5b%22%7D%7D',
            'https://api.cilabs.com/graphql?operationName=GetScheduleDay&variables=%7B%22conferenceId%22%3A%22ws19%22,%22dayId%22%3A%22MjAxOS0xMS0wNQ%3D%3D%22,%22timeFormat%22%3A%22%25H%3A%25M%3A%25S%22%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1,%22sha256Hash%22%3A%22060f890092d04ad9ce812ba20b5b1d9dcc03bab04cc1a1e40257858b99082b5b%22%7D%7D',
            'https://api.cilabs.com/graphql?operationName=GetScheduleDay&variables=%7B%22conferenceId%22%3A%22ws19%22,%22dayId%22%3A%22MjAxOS0xMS0wNg%3D%3D%22,%22timeFormat%22%3A%22%25H%3A%25M%3A%25S%22%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1,%22sha256Hash%22%3A%22060f890092d04ad9ce812ba20b5b1d9dcc03bab04cc1a1e40257858b99082b5b%22%7D%7D',
            'https://api.cilabs.com/graphql?operationName=GetScheduleDay&variables=%7B%22conferenceId%22%3A%22ws19%22,%22dayId%22%3A%22MjAxOS0xMS0wNw%3D%3D%22,%22timeFormat%22%3A%22%25H%3A%25M%3A%25S%22%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1,%22sha256Hash%22%3A%22060f890092d04ad9ce812ba20b5b1d9dcc03bab04cc1a1e40257858b99082b5b%22%7D%7D'
        ];
        this.urlIndex = 0;
        this.cache = [];
    }

    _read(size) {
        if (this.cache.length > 0) {
            this.push(this.cache.shift());
            return;
        }
        if (this.urlIndex >= this.urls.length) {
            this.push(null);
            return;
        }

        console.debug('populating cache', this.urls[this.urlIndex]);

        request({
            uri: this.urls[this.urlIndex],
            json: true,
        }).then(response => {
            ++this.urlIndex;
            const stages = response.data.conference.schedule.day.stages.nodes.map(stage => stage.timeslots.nodes);
            const talks = stages.reduce((a, b) => a.concat(b), []);
            this.cache = this.cache.concat(talks);
            console.debug('cache size', this.cache.length);
            this.push(this.cache.shift());
        }).catch(error => {
            this.emit('error', error);
        });

    }

};

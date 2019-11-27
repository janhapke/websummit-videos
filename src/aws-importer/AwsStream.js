const Stream = require('stream');
const request = require('request-promise');
const cheerio = require('cheerio');

module.exports = class AwsStream extends Stream.Readable {

    constructor() {
        super({objectMode: true});
        this.url = 'https://emea-resources.awscloud.com/aws-developer-workshop-theater-websummit-2019';
        this.initialized = false;
        this.cache = [];
    }

    _read(size) {
        if (this.cache.length > 0) {
            this.push(this.cache.shift());
            return;
        }

        if (this.initialized && this.cache.length == 0) {
            this.push(null);
            return;
        }

        console.debug('populating cache', this.url);

        request({
            uri: this.url,
        }).then(response => {
            const $ = cheerio.load(response);

            const slides = $('.tile.uberflip').map(
                    (idx, elem) => {
                        return {
                            title: $('.h3like', elem).text(),
                            link: $('a.item-link.view', elem).attr('href'),
                            image_url: $('img', elem).attr('src')
                        };
                    }
                ).toArray();

            this.cache = this.cache.concat(slides);
            console.debug('cache size', this.cache.length);
            this.initialized = true;
            this.push(this.cache.shift());
        }).catch(error => {
            this.emit('error', error);
        });

    }

};

const Stream = require('stream');

module.exports = class VimeoStream extends Stream.Readable {

    constructor(vimeoClient, startPath, waitBetweenRequests = 1000) {
        super({objectMode: true});
        this.vimeoClient = vimeoClient;
        this.nextUrl = startPath;
        this.waitBetweenRequests = waitBetweenRequests;
        this.cache = [];
    }

    _read(size) {
        if (this.cache.length > 0) {
            this.push(this.cache.shift());
            return;
        }

        if (!this.nextUrl) {
            console.debug('no next url, finish stream')
            this.push(null);
            return;
        }

        console.log('cache empty - making api request', this.nextUrl);

        setTimeout(() => {
            this.vimeoClient.request({
                method: 'GET',
                path: this.nextUrl
                }, (error, body, status_code, headers) => {
                if (error) {
                    console.error('api error', error);
                    return;
                }
                if (!body.data.length) {
                    return;
                }
                this.cache = this.cache.concat(body.data);
                this.nextUrl = body.paging.next;
                this.push(this.cache.shift());
            });
        }, this.waitBetweenRequests);
    }

};

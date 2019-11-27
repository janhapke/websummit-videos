const Stream = require('stream');
const slugify = require('slugify');

function slugifyString(string) {
    return slugify(string.replace('Q+A', 'Q&A').toLowerCase(), {remove: /[*,\/\?´+~.()'"!:@]/g}).trim();
}

module.exports = class TransformStream extends Stream.Transform {

    constructor() {
        super({ readableObjectMode: true, writableObjectMode: true});
    }

    _transform(chunk, encoding, callback) {
        this.push({
            slug: slugifyString(chunk.title),
            title: chunk.title,
            link: chunk.link,
            image_url: chunk.image_url,
        });
        callback();
    }

};

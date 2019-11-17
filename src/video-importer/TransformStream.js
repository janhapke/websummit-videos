const Stream = require('stream');

module.exports = class TransformStream extends Stream.Transform {

    constructor() {
        super({ readableObjectMode: true, writableObjectMode: true});
    }

    _transform(chunk, encoding, callback) {
        this.push({
            uri: chunk.uri,
            name: chunk.name,
            description: chunk.description,
            link: chunk.link,
            duration_sec: chunk.duration,
            release_time: chunk.release_time,
            pic_small: this._findPictureForWidth(chunk, 295),
            pic_medium: this._findPictureForWidth(chunk, 960),
            pic_large: this._findPictureForWidth(chunk, 1920),
        });
        callback();
    }

    _findPictureForWidth(chunk, width) {
        if (!chunk.pictures || !chunk.pictures.sizes || !Array.isArray(chunk.pictures.sizes)) {
            return null;
        }
        const matches = chunk.pictures.sizes.filter(size => size.width == width);
        if (!matches[0]) {
            return null;
        }
        return matches[0].link;
    }
};

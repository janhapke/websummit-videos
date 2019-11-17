const Stream = require('stream');

module.exports = class TransformStream extends Stream.Transform {

    constructor() {
        super({ readableObjectMode: true, writableObjectMode: true});
    }

    _transform(chunk, encoding, callback) {
        this.push({
            id: chunk.id,
            stage: chunk.location.name,
            title: chunk.title,
            description: chunk.description,
            date: chunk.startDate,
            start_time: chunk.startTime,
            end_time: chunk.endTime,
            duration_sec: chunk.duration * 60,
            topics: chunk.topics.nodes.map(node => node.name),
            presenters: chunk.participants.nodes.map(node => {
                return {
                    name: node.attendee.name,
                    companyName: node.attendee.companyName,
                    jobTitle: node.attendee.jobTitle,
                    bio: node.attendee.bio,
                }
            }),
        });
        callback();
    }

};

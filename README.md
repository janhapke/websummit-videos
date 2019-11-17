# Web Summit Videos
The Videos from [Web Summit](https://websummit.com) have been posted to the [Web Summit Vimeo Account](https://vimeo.com/websummit).

However, I had difficulties finding some of the videos I was looking for.

That's why I created this little tool that fetches data from the Web Summit API as well as the Vimeo API and matches both together.

The output is a long list of Web Summit Talks together with the videos where I could match them.

Find the result here: [Web Summit Videos](https://janhapke.github.io/websummit-videos/)

## Development Setup
### Overview
The page is generated through these steps:

  * Data is stored inside an SQLite Databse `database.sqlite`
  * Videos are imported from the Web Summit Vimeo Account via the Vimeo API
    * stored inside the table `videos`
  * Metadata of the Talks at Web Summit is imported from the Web Summit API
    * stored inside the table `talks`
  * Talks and Videos are matched
    * stored inside the table `matches`
  * An HTML page with the result is generated
    * stored as `index.html`

### Vimeo API Credentials
Follow the [Getting Started Guide](https://developer.vimeo.com/api/guides/start) for the Vimeo API to get your API credentials.

Create a new file `config.js` by copying `config.dist.js` and modifying the contents with your API credentials

### Install dependencies

    npm install

### Import Videos

    npm run import-videos

### Import Web Summit Sessions

    npm run import-schedule

### Match Sessions with Videos

    npm run match

### Generate HTML page with result

    npm run export

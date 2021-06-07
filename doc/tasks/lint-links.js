
const path = require('path')
const hyperlink = require('hyperlink')
const TapRender = require('@munter/tap-render');

const root = path.join(__dirname, '..')
console.log(root)

;(async () => {
  const tapRender = new TapRender()
  tapRender.pipe(process.stdout)
  try {
    const skipPatterns = [
      // initial redirect
      'load index.html',
      'load docs/index.html',
      // static
      'load build/site/developer/_',
      // google fonts
      'load https://fonts.googleapis.com/',
      // static resources
      'load static/assets',
      // externals links
      // /
      'load try-neo4j',
      // /developer
      'load developer',
      // /labs
      'load labs',
      // /docs
      'load docs',
      // rate limit on twitter.com (will return 400 code if quota exceeded)
      'external-check https://twitter.com/neo4j',
      // workaround: not sure why the following ids are not resolved properly by hyperlink :/
      'https://kafka.apache.org/documentation/#brokerconfigs',
      'https://kafka.apache.org/documentation/#streamsapi',
      'https://kafka.apache.org/documentation.html#compaction',
      // ignore edit-link
      'external-check https://github.com/neo4j-contrib/neo4j-streams/edit/',
      'external-check neo4j-contrib/neo4j-streams/edit/',
      // graphgist
      'load graphgist',
    ]
    const skipFilter = (report) => {
      return Object.values(report).some((value) => {
          return skipPatterns.some((pattern) => String(value).includes(pattern))
        }
      )
    };
    await hyperlink({
        root,
        inputUrls: [`build/site/labs/kafka/4.0/index.html`],
        skipFilter: skipFilter,
        recursive: true,
      },
      tapRender
    )
  } catch (err) {
    console.log(err.stack);
    process.exit(1);
  }
  const results = tapRender.close();
  process.exit(results.fail ? 1 : 0);
})()


//hyperlink --skip 'developer/data-modeling' --skip 'developer/cypher' --skip '/developer/graph-platform' --skip '/developer/get-started' --skip '/docs' --skip '/developer/guide-cloud-deployment' --skip '/developer/neo4j-apoc' --skip '/spark/try-neo4j' --skip '/spark/labs' --skip '/wp-content/themes/neo4jweb/favicon.ico' --skip '/spark/static/assets' --skip 'load build/site/labs/apoc/' --skip 'load https://uglfznxroe.execute-api.us-east-1.amazonaws.com/dev/Feedback' --skip 'fontawesome-webfont.eot' --skip 'load /guide-performance-tuning' --skip 'external-check https://twitter.com/neo4j' build/site/developer/spark/*.html

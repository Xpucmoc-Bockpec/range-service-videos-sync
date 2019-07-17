module.exports = {
	apps: [{
		name: "api-service-videos-sync",
		script: "index.js",
		env: {
			YOUTUBE_API_KEY: "secret"
			VIDEOS_CHUNK_SIZE: 10,
			RABBIT_URL: "amqp://username:password@127.0.0.1:5672",
			RABBIT_UNPROCESSED_QUEUE_NAME: "unprocessed-youtube-videos",
			RABBIT_PROCESSED_QUEUE_NAME: "processed-youtube-videos"
		}
	}]
}
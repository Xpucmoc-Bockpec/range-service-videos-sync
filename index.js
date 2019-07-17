const amqp = require("amqplib");
const https = require("https");
const ISODuration = require("iso8601-duration");

const queueUnprocessed = "unprocessed-youtube-videos";
const queueProcessed = "processed-youtube-videos";
const VIDEOS_CHUNK_SIZE = 10;

const state = {
	connection: amqp.connect(`amqp://${process.env.username}:${process.env.password}@${process.env.hostname}:${process.env.port}`),
	publisher: null,
	consumer: null
};

(async function() {
	state.consumer = await createChannel()
		.then(async channel => {
			await channel.assertQueue(queueUnprocessed, { durable: true });
			await channel.prefetch(VIDEOS_CHUNK_SIZE);
			return channel;
		});
		
	state.publisher = await createChannel()
		.then(async channel => {
			await channel.assertQueue(queueProcessed);
			return channel;
		});

	setInterval(async () => await getVideosChunk(), 3000);
})();

async function getVideosChunk() {
	const chunk = [];
	
	for (let i = 0; i < VIDEOS_CHUNK_SIZE; i++) {
		const message = await state.consumer.get(queueUnprocessed);
		
		if (!message) break;
		chunk.push({
			parsedMessage: JSON.parse(message.toString()),
			rawMessage
		});
	}

	await processVideosChunk(chunk);
}

async function processVideosChunk(chunk) {
	const videoIds = chunk.map(({ parsedMessage }) => parsedMessage.youTubeId);
	const youTubeMeta = await getVideosData(videoIds);
	
	for (const { parsedMessage, rawMessage } of chunk) {
		const { status, contentDetails, snippet } = youTubeMeta.find(d => d.id === parsedMessage.youTubeId) || {}; // YouTube API ignores videos removed due to duplicate
		
		if (!status || status.uploadStatus === "failed") {
			await state.publisher.sendToQueue(queueProcessed, {
				id: parsedMessage.id,
				removed: true
			});
			await state.consumer.ack(rawMessage);
		}
		else if (status.uploadStatus === "processed") {
			await state.publisher.sendToQueue(queueProcessed, {
				id: parsedMessage.id,
				duration: ISODuration.toSeconds(ISODuration.parse(contentDetails.duration)),
				thumbnails: snippet.thumbnails
			});
			await state.consumer.ack(rawMessage);
		}
		else await state.consumer.nack(rawMessage);
	}
}
		
		
function getVideosData(videoIds) {
	const options = {
		method: "GET",
		hostname: "www.googleapis.com",
		port: 443,
		path: `/youtube/v3/videos?id=${videoIds.join(",")}&key=${process.env.apiKey}&part=status,snippet,contentDetails`
	};
	
	return new Promise((resolve, reject) => {
		let response = "";
		
		const request = https.request(options, (res) => {
			res.on("data", data => (response += data));
			res.on("end", () => {
				try {
					resolve(JSON.parse(response).items);
				}
				catch (e) {
					reject(`Invalid YouTube API response:\n\t${response}`);
				}
			});
		});
		
		request.on("error", error => reject(`Failed to fetch YouTube API:\n\t${error}`));
		request.end();
	});
}

async function createChannel() {
	const chanel = await state.connection.createChannel();
	const sendToQueue = channel.sendToQueue.bind(channel);
	channel.sendToQueue = (queue, content, sendToQueueOptions) => sendToQueue(queue, Buffer.from(JSON.stringify(content)), sendToQueueOptions);
	
	return channel;
}


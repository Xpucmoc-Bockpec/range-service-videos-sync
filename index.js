const amqp = require("amqplib");
const https = require("https");
const ISODuration = require("iso8601-duration");

const queueUnprocessed = "unprocessed-youtube-videos";
const queueProcessed = "processed-youtube-videos";
const MAX_VIDEOS_COUNT = 10;

const state = {
	connection: amqp.connect(`amqp://${process.env.username}:${process.env.password}@${process.env.hostname}:${process.env.port}`),
	publisher: null,
	consumer: null
};

(async function() {
	state.consumer = await createChannel()
		.then(async channel => {
			await channel.assertQueue(queueUnprocessed, { durable: true });
			await channel.prefetch(MAX_VIDEOS_COUNT);
			return channel;
		});
		
	state.publisher = await createChannel()
		.then(async channel => {
			await channel.assertQueue(queueProcessed);
			return channel;
		});

	setInterval(async () => await getQueuedVideos(), 3000);
})();

async function getQueuedVideos() {
	const messages = [];
	
	for (let i = 0; i < MAX_VIDEOS_COUNT; i++) {
		const message = await state.consumer.get(queueUnprocessed);
		
		if (!message) break;
		messages.push({
			parsedData: JSON.parse(message.toString()),
			rawMessage
		});
	}

	await processVideos(messages);
}

async function processVideos(messages) {
	const videoIds = messages.map(({ parsedData }) => parsedData.youTubeId);
	const youTubeMeta = await getVideosData(videoIds);
	
	for (const message of messages) {
		const { video, rawMessage } = message;
		const { status, contentDetails, snippet } = youTubeMeta.find(d => d.id === video.youTubeId) || {}; // YouTube API ignores videos removed due to duplicate
		
		if (!status || status.uploadStatus === "failed") {
			await state.publisher.sendToQueue(queueProcessed, {
				id: video.id,
				removed: true
			});
			await state.consumer.ack(message);
		}
		else if (status.uploadStatus === "processed") {
			await state.publisher.sendToQueue(queueProcessed, {
				id: video.id,
				duration: ISODuration.toSeconds(ISODuration.parse(contentDetails.duration)),
				thumbnails: snippet.thumbnails
			});
			await state.consumer.ack(message);
		}
		else await state.consumer.nack(message);
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
					reject("youtube api отправил некорректный ответ");
				}
			});
		});
		
		request.on("error", () => reject("не удалось завершить запрос к youtube api"));
		request.end();
	});
}

async function createChannel() {
	const chanel = await state.connection.createChannel();
	const sendToQueue = channel.sendToQueue.bind(channel);
	channel.sendToQueue = (queue, content, sendToQueueOptions) => sendToQueue(queue, Buffer.from(JSON.stringify(content)), sendToQueueOptions);
	
	return channel;
}


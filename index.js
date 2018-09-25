const amqp = require("amqplib");
const https = require("https");
const ISODuration = require("iso8601-duration");

const queueUnprocessed = "unprocessed-youtube-videos";
const queueProcessed = "processed-youtube-videos";
const MAX_VIDEOS_COUNT = 10;


const client = amqp.connect(`amqp://${process.env.username}:${process.env.password}@${process.env.hostname}:${process.env.port}`);

/*
	Получает сообщения { id: objectid, youTubeId: string }
*/
const Consumer = client
	.then(connection => connection.createChannel());

/*
	После того, как получили инфу о видео via YouTube API,
	добавляем в очередь обработтаных видеозаписей
*/
const Publisher = client
	.then(connection => connection.createChannel())
	.then(async channel => {
		await channel.assertQueue(queueProcessed);
		return channel;
	});
	

Promise
	.all([ Consumer, Publisher ])
	.then(async ([ consumerChannel, publisherChannel ]) => {
		await consumerChannel
			.assertQueue(queueUnprocessed, { durable: true })
			.then(() => consumerChannel.prefetch(MAX_VIDEOS_COUNT));

		// тупа ебашим на заводи
		setInterval(async () => {	
			let messages = [];
			
			for (let i = 0; i < MAX_VIDEOS_COUNT; i++) {
				const message = await consumerChannel.get(queueUnprocessed);
				if (!message) break;
				messages.push(message);
			}
			
			if (messages.length !== 0) {
				await processMessages(messages, consumerChannel, publisherChannel);
			}
		}, 3000);
	});


async function processMessages(messages, consumerChannel, publisherChannel) {
	const videoIds = [];
	
	for (const message of messages) {
		const video = JSON.parse(message.content.toString());
		videoIds.push(video.youTubeId);
	}
	
	const recievedVideosInfo = await getVideosInfo(videoIds);
	for (const message of messages) {
		const video = JSON.parse(message.content.toString());
		// гугл не возвращает в массиве видео, которые удалены из-за копии, хотя обычно ставит им статус failed
		const recievedVideoInfo = recievedVideosInfo.find(d => d.id === video.youTubeId); 
		
		if (!recievedVideoInfo || recievedVideoInfo.status.uploadStatus === "failed") {
			const videoInfoMessage = { 
				id, 
				removed: true 
			};
					
			await publisherChannel.sendToQueue(queueProcessed, Buffer.from(JSON.stringify(videoInfoMessage)));
			await consumerChannel.ack(message);
		}
		else if (recievedVideoInfo.status.uploadStatus === "processed") {
			const videoInfoMessage = {
				id: video.id,
				duration: ISODuration.toSeconds(ISODuration.parse(recievedVideoInfo.contentDetails.duration)),
				thumbnails: recievedVideoInfo.snippet.thumbnails	
			};
				
			await publisherChannel.sendToQueue(queueProcessed, Buffer.from(JSON.stringify(videoInfoMessage)));
			await consumerChannel.ack(message);
		}
		else await consumerChannel.nack(message);
	}
}
		
		
function getVideosInfo(videoIds) {
	const options = {
		method: "GET",
		hostname: "www.googleapis.com",
		port: 443,
		path: `/youtube/v3/videos?id=${videoIds.join(",")}&key=${process.env.apiKey}&part=status,snippet,contentDetails`
	};
	
	return new Promise((resolve, reject) => {
		let response = "";
		
		const request = https.request(options, (res) => {
			res.on("data", (data) => {
				response += data
			});
			
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


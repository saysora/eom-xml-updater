import axios from 'axios';
import { XMLParser } from 'fast-xml-parser';
import { Client } from 'pg';

interface ItunesXMLItem {
	title: string;
	'itunes:author': string;
	'itunes:summary': string;
	'itunes:duration': string;
	description?: string;
	pubDate: string;
	guid: string;
	enclosure?: {
		['@_url']?: string;
	};
}

interface XMLItem {
	title: string;
	author: string;
	description: string;
	url: string;
	duration: string;
	durationSeconds: number;
	url_type: string;
	pubDate: string;
	formattedDate: string;
	filename: string;
}

const wwk = 'Women Worth Knowing';

function parseSeconds(timeString: string) {
	timeString = String(timeString);
	const [hours, minutes, seconds] = timeString.split(':');

	if (!hours || !minutes || !seconds) {
		return parseInt(timeString);
	}

	return parseInt(hours) * 3600 + parseInt(minutes) * 60 + parseInt(seconds);
}

function parseFileName(name: string) {
	const filePieces = name.split('/');
	const fileName = filePieces[filePieces.length - 1];

	return fileName?.split('.')?.[0];
}

function parseFileType(name: string): string {
	const filePieces = name.split('.');
	const file_type = filePieces[filePieces.length - 1];

	return file_type;
}

export default {
	async fetch(req) {
		const url = new URL(req.url);
		url.pathname = '/__scheduled';
		url.searchParams.append('cron', '* * * * *');
		return new Response(`To test the scheduled handler, ensure you have used the "--test-scheduled" then try running "curl ${url.href}".`);
	},

	// The scheduled handler is invoked at the interval set in our wrangler.jsonc's
	// [[triggers]] configuration.
	async scheduled(event, env, ctx): Promise<void> {
		const xmls = env.XML_URLS;

		for (const xml of xmls) {
			const { data } = await axios.get(xml);

			const xmlParser = new XMLParser({ ignoreAttributes: false });

			const feedJson = JSON.stringify(xmlParser.parse(data), null, 4);

			const xmlName = JSON.parse(feedJson)?.rss?.channel?.title;

			console.info(`Processing items for ${xmlName}`);

			let xmlItems: XMLItem[] = [];

			if (xmlName.trim() === wwk) {
				xmlItems =
					JSON.parse(feedJson)?.rss?.channel?.item?.map((i: ItunesXMLItem) => {
						const date = new Date(i.pubDate);
						const formattedDate = `${date.getUTCFullYear()}-${date.getUTCMonth() + 1}-${date.getUTCDate()}`;

						const upgradedUrl = i.enclosure?.['@_url']?.replace('http://', 'https://');

						return {
							title: i.title.trim(),
							author: i['itunes:author'].trim(),
							description: i.description?.trim(),
							duration: i['itunes:duration'],
							durationSeconds: parseSeconds(i['itunes:duration']),
							url: upgradedUrl,
							url_type: parseFileType(upgradedUrl ?? 'mp3'),
							filename: parseFileName(upgradedUrl ?? ''),
							pubDate: i.pubDate,
							formattedDate,
						};
					}) ?? [];
			} else {
				xmlItems =
					JSON.parse(feedJson)?.rss?.channel?.item?.map((i: ItunesXMLItem) => {
						const date = new Date(i.pubDate);
						const formattedDate = `${date.getUTCFullYear()}-${date.getUTCMonth() + 1}-${date.getUTCDate()}`;

						const upgradedUrl = i.guid.replace('http://', 'https://');

						return {
							title: i.title.trim(),
							author: i['itunes:author'].trim(),
							description: i['description']?.trim(),
							duration: i['itunes:duration'],
							durationSeconds: parseSeconds(i['itunes:duration']),
							url: upgradedUrl,
							url_type: parseFileType(upgradedUrl),
							filename: parseFileName(upgradedUrl),
							pubDate: i.pubDate,
							formattedDate,
						};
					}) ?? [];
			}

			xmlItems = xmlItems.sort((a, b) => {
				const aDate = new Date(a?.pubDate).getTime();
				const bDate = new Date(b?.pubDate).getTime();

				return aDate - bDate;
			});

			const itemsToCheck = xmlItems.slice(xmlItems.length - 10);

			if (!itemsToCheck.length) {
				console.log('No items to parse');
				return;
			}

			const sql = new Client({ connectionString: env.DB_URL });

			await sql.connect();

			// Get the author for this podcast
			const authorArr = await sql.query('SELECT id, name from author');
			const authorMap: Record<string, number> = {};

			for (const author of authorArr.rows) {
				authorMap[author.name] = parseInt(author.id);
			}

			// Get the series this podcast is from
			const seriesArr = await sql.query('SELECT id, title from series');
			const seriesMap: Record<string, number> = {};

			for (const series of seriesArr.rows) {
				if (series.title == 'Back to Basics Radio') {
					seriesMap['Back to Basics'] = parseInt(series.id);
				}
				seriesMap[series.title] = parseInt(series.id);
			}

			console.log(`Running cron for ${new Date().toDateString()}`);
			for (const item of itemsToCheck) {
				const result = await sql.query('SELECT * from item where filename = $1 AND pub_date = $2', [item.filename, item.formattedDate]);

				if (result.rows?.[0]) {
					console.log(`${item.filename} exists, skipping`);
					continue;
				}

				console.log(`item ${item.title} does not exist, adding it now`);
				let itemArgs: (string | number)[] = [];
				let insertString = '';

				if (xmlName.trim() === wwk) {
					const people = item.author.split(',');
					const authors = people.map((p) => p.trim());

					itemArgs = [
						item.title,
						item.description,
						item.durationSeconds,
						item.url,
						item.formattedDate,
						item.formattedDate,
						authorMap['Cheryl Brodersen'],
						item.filename,
						item.url_type,
						authors.filter((a) => a !== 'Cheryl Brodersen').join(', '),
					];
					insertString = `
						INSERT INTO item
							(
								title,
								description,
								duration,
								url,
								date,
								pub_date,
								author_id,
								filename,
								url_type,
								co_authors
							) VALUES (
								$1,
								$2,
								$3,
								$4,
								$5,
								$6,
								$7,
								$8,
								$9,
								$10
							) RETURNING id
`;
				} else {
					itemArgs = [
						item.title,
						item.description,
						item.durationSeconds,
						item.url,
						item.formattedDate,
						item.formattedDate,
						authorMap[item.author],
						item.filename,
						item.url_type,
					];

					insertString = `
						INSERT INTO item
							(
								title,
								description,
								duration,
								url,
								date,
								pub_date,
								author_id,
								filename,
								url_type
							) VALUES (
								$1,
								$2,
								$3,
								$4,
								$5,
								$6,
								$7,
								$8,
								$9
							) RETURNING id
`;
				}

				try {
					const studyInsert = await sql.query(insertString, itemArgs);

					if (!studyInsert.rows.length) {
						console.log('No rows inserted, so moving on');
						return;
					}

					// Now we create the associated series item record
					console.log(`Creating series relation to ${xmlName}`);
					await sql.query(
						'INSERT INTO series_item (series_id, item_id) VALUES ($1, $2) ON CONFLICT ON CONSTRAINT unique_series_and_item DO NOTHING',
						[seriesMap[xmlName.trim()], studyInsert.rows?.[0].id],
					);
				} catch (e) {
					console.error('Could not write file data to db', e);
				}
			}
		}
	},
} satisfies ExportedHandler<Env>;

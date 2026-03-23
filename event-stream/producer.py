import json
from requests_sse import EventSource

url = 'https://stream.wikimedia.org/v2/stream/recentchange'
headers = {"User-Agent": "Wikimedia-Analytics/0.1 nacht29.study@gmail.com"}

with EventSource(url, headers=headers) as stream:
	for event in stream:
		with open("event-payload.json", 'w', encoding='utf-8') as event_file:
			event_file.write(json.loads(event.data))
		if event.type == 'message':
			try:
				change = json.loads(event.data)
			except ValueError:
				pass
			else:
				if change['meta']['domain'] == 'canary':
					continue
				print(f"{change['user']} edited {change['title']}")

'''
from pywikibot.comms.eventstreams import EventStreams
stream = EventStreams(streams=['recentchange', 'revision-create'], since='20250107')
stream.register_filter(server_name='fr.wikipedia.org', type='edit')
change = next(stream)
print('{type} on page "{title}" by "{user}" at {meta[dt]}.'.format(**change))
'''

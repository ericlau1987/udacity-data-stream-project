# Please complete the TODO items in the code

from dataclasses import asdict, dataclass, field
import json
from faker import Faker
import faust

faker = Faker()
@dataclass
class ClickEvent(faust.Record):
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)
    number: int = field(default_factory=lambda: random.randint(0, 999))


@dataclass
class ClickEventSanitized(faust.Record):
    timestamp: str
    uri: str
    number: int


app = faust.App("exercise3", broker="kafka://kafka0:19092")
clickevents_topic = app.topic("lesson4.solution5.click_events", 
    key_type=str,
    value_type=ClickEvent
)

#
# TODO: Define an output topic for sanitized click events, without the user email
#
sanitized_topic = app.topic(
    "com.udacity.streams.clickevents.sanitized", 
    key_type=str,
    value_type=ClickEventSanitized)

@app.agent(clickevents_topic)
async def clickevent(clickevents):
    async for clickevent in clickevents:
        #
        # TODO: Modify the incoming click event to remove the user email.
        #       Create and send a ClickEventSanitized object.
        sanitized = ClickEventSanitized(
            timestamp=click_event.timestamp,
            uri=click_event.uri,
            number=click_event.number
        )

        #
        # TODO: Send the data to the topic you created above.
        #       Make sure to set a key and value
        #
        await sanitized_topic.send(key=sanitized.uri, value=sanitized)

if __name__ == "__main__":
    app.main()

# Please complete the TODO items in the code

from dataclasses import asdict, dataclass
import json
import random

import faust


@dataclass
class ClickEvent(faust.Record):
    email: str
    timestamp: str
    uri: str
    number: int
    score: float = 0


#
# TODO: Define a scoring function for incoming ClickEvents.
#       It doens't matter _how_ you score the incoming records, just perform
#       some modification of the `ClickEvent.score` field and return the value
#
def add_score(ClickEvent) -> json:
    ClickEvent.score = random.randint(0, 100)
    return ClickEvent


app = faust.App("exercise5", broker="kafka://kafka0:19092")
clickevents_topic = app.topic("lesson4.solution5.click_events", value_type=ClickEvent)
scored_topic = app.topic(
    "com.udacity.streams.clickevents.scored",
    key_type=str,
    value_type=ClickEvent,
)

@app.agent(clickevents_topic)
async def clickevent(clickevents):
    #
    # TODO: Add the `add_score` processor to the incoming clickevents
    #       See: https://faust.readthedocs.io/en/latest/reference/faust.streams.html?highlight=add_processor#faust.streams.Stream.add_processor
    #
    clickevents.add_processor(add_score)
    async for ce in clickevents:
        await scored_topic.send(key=ce.uri, value=ce)

if __name__ == "__main__":
    app.main()
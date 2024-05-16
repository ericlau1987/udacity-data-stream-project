# Please complete the TODO items in the code

from dataclasses import asdict, dataclass
from datetime import timedelta
import json
import random

import faust


@dataclass
class ClickEvent(faust.Record):
    email: str
    timestamp: str
    uri: str
    number: int


app = faust.App("exercise7", broker="kafka://kafka0:19092")
clickevents_topic = app.topic("lesson4.solution5.click_events", value_type=ClickEvent)

#
# TODO: Define a tumbling window of 10 seconds
#       See: https://faust.readthedocs.io/en/latest/userguide/tables.html#how-to
#
uri_summary_table = app.Table("uri_summary_tumbling", default=int).tumbling(
    timedelta(seconds=30)
)


@app.agent(clickevents_topic)
async def clickevent(clickevents):
    async for ce in clickevents.group_by(ClickEvent.uri):
        uri_summary_table[ce.uri] += ce.number
        #
        # TODO: Play with printing value by: now(), current(), value()
        #       See: https://faust.readthedocs.io/en/latest/userguide/tables.html#how-to
        #
        print(f"{ce.uri}: {uri_summary_table[ce.uri].current()}")


if __name__ == "__main__":
    app.main()
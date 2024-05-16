# Please complete the TODO items in the code

from dataclasses import asdict, dataclass, field
import json
from faker import Faker
import faust

faker = Faker()
#
# TODO: Define a ClickEvent Record Class with an email (str), timestamp (str), uri(str),
#       and number (int)
#
#       See: https://docs.python.org/3/library/dataclasses.html
#       See: https://faust.readthedocs.io/en/latest/userguide/models.html#model-types
#
@dataclass
class ClickEvent(faust.Record):
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)
    number: int = field(default_factory=lambda: random.randint(0, 999))

app = faust.App("exercise2", broker="kafka://kafka0:19092")

#
# TODO: Provide the key (uri) and value type to the clickevent
#
clickevents_topic = app.topic(
    "lesson4.solution5.click_events",
    key_type=str,
    value_type=ClickEvent,
)

@app.agent(clickevents_topic)
async def clickevent(clickevents):
    async for ce in clickevents:
        print(json.dumps(asdict(ce), indent=2))


if __name__ == "__main__":
    app.main()

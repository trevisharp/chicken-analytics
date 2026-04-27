import json
import uuid
import random
from datetime import datetime, timedelta
from pathlib import Path

OUTPUT_DIR = Path("data/bronze")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def new_guid():
    return str(uuid.uuid4())

def random_timestamp(start, end):
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)

def now_iso(dt):
    return dt.isoformat()

published = 0
def publish(event):
    global published
    published += 1

    file_name = f"{event['event_id']}.json"
    output_file = OUTPUT_DIR / file_name

    with open(output_file, "w") as f:
        json.dump(event, f)

def buy_eggs(time):
    event = {
        "event_id": new_guid(),
        "event_type": "egg_purchase",
        "egg_batch_id": new_guid(),
        "timestamp": now_iso(time),
        "quantity": 200 + 25 * random.randint(0, 4),
        "matriz_farm": random.choice([ "matrizA", "matrizB", "matrizC" ])
    }
    publish(event)
    return event

def send_chicks(time, egg_batch, count):
    event = {
        "event_id": new_guid(),
        "event_type": "send_chicks",
        "egg_batch_id": egg_batch,
        "chick_batch_id": new_guid(),
        "timestamp": now_iso(time),
        "quantity": count
    }
    publish(event)
    return event

def call_slaughter(time, chick_batch, count):
    event = {
        "event_id": new_guid(),
        "event_type": "slaughter",
        "chick_batch_id": chick_batch,
        "slaughter_batch_id": new_guid(),
        "timestamp": now_iso(time),
        "quantity": count
    }
    publish(event)
    return event


def main():
    date = datetime.now() - timedelta(days=180)

    egg_stock = 0
    egg_batch = 0

    incubation = []
    fattening = []

    while date < datetime.now():
        
        if egg_stock == 0:
            ev = buy_eggs(date)
            egg_batch = ev["egg_batch_id"]
            egg_stock = ev["quantity"]
        date += timedelta(days=7)

        for i in range(len(fattening)):
            crr = fattening[i]
            if crr[2] + random.randint(0, 5) > 50:
                call_slaughter(date, crr[1], crr[0])
                fattening.pop()
                i -= 1
                pass

            fattening[i] = (crr[0], crr[1], crr[2] - 7)
        
        new_incubation = min(60, egg_stock)
        incubation.append((new_incubation, egg_batch))
        egg_stock -= new_incubation

        if len(incubation) < 3:
            pass
        value = incubation.pop()
        
        chicks = value[0] - random.randint(0, min(value[0] // 2, 5))
        ev = send_chicks(date, value[1], chicks)
        fattening.append((chicks, ev["chick_batch_id"], random.randint(40, 50)))

    global published
    print(f"Generated {published} events at {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
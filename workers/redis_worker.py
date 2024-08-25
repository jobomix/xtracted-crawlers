import asyncio

import redis.asyncio as redis

started = False

background_tasks = set()


async def start() -> None:
    global started
    started = True
    consuming = asyncio.create_task(consume())
    background_tasks.add(consuming)
    consuming.add_done_callback(background_tasks.discard)


async def stop() -> None:
    global started
    started = False


async def consume() -> None:
    client = redis.StrictRedis(decode_responses=True)

    while True:
        r = await client.xread(streams={'crawl': '$'}, count=1, block=1000)
        if len(r) == 1:
            res = r[0][1]
            data = res[0][1]
            url = data['url']
            job_id = data['job_id']
            context = await client.hgetall(f'job:{job_id}:{url}')  # type: ignore
            context['status'] = 'STARTED'
            await client.hset(name=f'job:{job_id}:{url}', mapping=context)  # type: ignore
        if not started:
            await asyncio.sleep(1)
            break


if __name__ == '__main__':
    asyncio.run(start())

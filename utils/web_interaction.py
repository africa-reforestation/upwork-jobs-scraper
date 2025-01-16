import asyncio

async def scroll_to_bottom(page, delay=1000):
    previous_height = await page.evaluate("document.body.scrollHeight")
    while True:
        await page.mouse.wheel(0, 1000)
        await page.wait_for_timeout(delay)
        new_height = await page.evaluate("document.body.scrollHeight")
        if new_height == previous_height:
            break
        previous_height = new_height

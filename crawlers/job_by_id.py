from tasks import hello


def on_raw_message(body):
    print(body)


result = hello.apply_async(["https://ifconfig.me"])
print(result.get())
for child in result.children:
    x = child.get(on_message=on_raw_message, propagate=False)


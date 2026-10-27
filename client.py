import asyncio
import sys

async def send_messages(writer: asyncio.StreamWriter):
    """Асинхронный ввод и отправка сообщений на сервер."""
    loop = asyncio.get_event_loop()
    while True:
        # Чтение ввода пользователя без блокировки основного потока
        message = await loop.run_in_executor(None, sys.stdin.readline)
        if not message:
            break
            
        writer.write(message.encode('utf-8'))
        await writer.drain()

        if message.strip() == "/quit":
            break

async def receive_messages(reader: asyncio.StreamReader):
    """Асинхронное чтение и вывод входящих сообщений от сервера."""
    while True:
        data = await reader.readline()
        if not data:
            print("\nСоединение с сервером закрыто.")
            break
        print(data.decode('utf-8'), end='')

async def main():
    host = '127.0.0.1'
    port = 8888

    try:
        reader, writer = await asyncio.open_connection(host, port)
    except ConnectionRefusedError:
        print(f"Не удалось подключиться к серверу {host}:{port}. Убедитесь, что сервер запущен.")
        return

    # Запускаем две параллельные асинхронные задачи: на прием и на отправку
    receive_task = asyncio.create_task(receive_messages(reader))
    send_task = asyncio.create_task(send_messages(writer))

    done, pending = await asyncio.wait(
        [receive_task, send_task],
        return_when=asyncio.FIRST_COMPLETED
    )

    for task in pending:
        task.cancel()

    writer.close()
    await writer.wait_closed()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nЧат завершен.")
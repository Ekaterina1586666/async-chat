import asyncio
import datetime
import logging

# Настройка логирования событий сервера (Критерий К4)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Словарь для хранения подключенных клиентов: {writer: username}
clients = {}

async def broadcast(message: str, sender_writer=None):
    """
    Трансляция (бродкастинг) сообщения всем подключенным клиентам.
    Если указан sender_writer, сообщение отправляется всем, КРОМЕ отправителя.
    """
    if not clients:
        return

    # Подготавливаем байтовую строку для отправки
    encoded_message = (message + "\n").encode('utf-8')
    
    for writer in list(clients.keys()):
        if writer != sender_writer:
            try:
                writer.write(encoded_message)
                await writer.drain()
            except Exception as e:
                logging.error(f"Ошибка отправки пользователю {clients.get(writer)}: {e}")

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """
    Обработка отдельного подключения пользователя.
    """
    addr = writer.get_extra_info('peername')
    logging.info(f"Новое подключение с адреса {addr}")

    # 1. Запрос имени пользователя
    writer.write("Добро пожаловать в асинхронный чат!\nВведите ваше имя: ".encode('utf-8'))
    await writer.drain()

    username_data = await reader.readline()
    if not username_data:
        writer.close()
        await writer.wait_closed()
        return

    username = username_data.decode('utf-8').strip()
    if not username:
        username = f"User_{addr[1]}"

    # Сохраняем клиента
    clients[writer] = username
    logging.info(f"Пользователь '{username}' авторизован ({addr})")

    # Уведомляем всех о подключении нового клиента
    welcome_msg = f"[СЕРВЕР] Пользователь '{username}' присоединился к чату!"
    await broadcast(welcome_msg)
    
    writer.write(f"[СЕРВЕР] Вы успешно вошли как '{username}'. Доступные команды: /list, /quit\n".encode('utf-8'))
    await writer.drain()

    # 2. Основной цикл приема сообщений
    try:
        while True:
            data = await reader.readline()
            if not data:
                break  # Соединение разорвано

            message = data.decode('utf-8').strip()
            if not message:
                continue

            # Обработка чат-команд (Критерий К4)
            if message == "/quit":
                break
            elif message == "/list":
                online_users = ", ".join(clients.values())
                info_msg = f"[СЕРВЕР] Сейчас в чате ({len(clients)}): {online_users}\n"
                writer.write(info_msg.encode('utf-8'))
                await writer.drain()
                continue

            # Форматирование времени отправки
            now = datetime.datetime.now().strftime("%H:%M:%S")
            formatted_msg = f"[{now}] {username}: {message}"
            
            logging.info(f"Сообщение от {username}: {message}")
            # Рассылаем сообщение всем остальным участникам
            await broadcast(formatted_msg, sender_writer=writer)

    except asyncio.CancelledError:
        pass
    except Exception as e:
        logging.error(f"Ошибка в сессии пользователя {username}: {e}")
    finally:
        # 3. Отключение пользователя и уведомление участников
        if writer in clients:
            del clients[writer]
            leave_msg = f"[СЕРВЕР] Пользователь '{username}' покинул чат."
            logging.info(f"Пользователь '{username}' отключился.")
            await broadcast(leave_msg)

        writer.close()
        await writer.wait_closed()

async def main():
    host = '127.0.0.1'
    port = 8888
    
    server = await asyncio.start_server(handle_client, host, port)
    logging.info(f"Асинхронный чат-сервер запущен на {host}:{port}")

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nСервер остановлен вручную.")

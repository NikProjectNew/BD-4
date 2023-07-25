import psycopg2

def create_db(conn):
    with conn.cursor() as cur:
        # Создание таблицы clients (id, first_name, last_name, email)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(25) NOT NULL,
                last_name VARCHAR(25) NOT NULL,
                email VARCHAR(40) NOT NULL
            );
        """)

        # Создание таблицы phones (id, client_id, phone)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS phones (
                id SERIAL PRIMARY KEY,
                client_id INTEGER NOT NULL,
                phone VARCHAR(20) NOT NULL,
                FOREIGN KEY (client_id) REFERENCES clients (id) ON DELETE CASCADE
            );
        """)

def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO clients (first_name, last_name, email)
            VALUES (%s, %s, %s)
            RETURNING id;
        """, (first_name, last_name, email))
        client_id = cur.fetchone()[0]

        if phones:
            for phone in phones:
                add_phone(conn, client_id, phone)

        conn.commit()

def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO phones (client_id, phone)
            VALUES (%s, %s);
        """, (client_id, phone))

        conn.commit()

def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    with conn.cursor() as cur:
        if first_name:
            cur.execute("""
                UPDATE clients
                SET first_name = %s
                WHERE id = %s;
            """, (first_name, client_id))

        if last_name:
            cur.execute("""
                UPDATE clients
                SET last_name = %s
                WHERE id = %s;
            """, (last_name, client_id))

        if email:
            cur.execute("""
                UPDATE clients
                SET email = %s
                WHERE id = %s;
            """, (email, client_id))

        if phones:
            # Удаление телефонов клиента и добавление новых
            cur.execute("""
                DELETE FROM phones
                WHERE client_id = %s;
            """, (client_id,))

            for phone in phones:
                add_phone(conn, client_id, phone)

        conn.commit()

def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM phones
            WHERE client_id = %s AND phone = %s;
        """, (client_id, phone))

        conn.commit()

def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM phones
            WHERE client_id = %s;
        """, (client_id,))

        cur.execute("""
            DELETE FROM clients
            WHERE id = %s;
        """, (client_id,))

        conn.commit()

def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cur:
        query = """
            SELECT clients.id, first_name, last_name, email, array_agg(phone) AS phones
            FROM clients
            LEFT JOIN phones ON clients.id = phones.client_id
        """

        conditions = []
        parameters = []

        if first_name:
            conditions.append("first_name = %s")
            parameters.append(first_name)

        if last_name:
            conditions.append("last_name = %s")
            parameters.append(last_name)

        if email:
            conditions.append("email = %s")
            parameters.append(email)

        if phone:
            conditions.append("phone = %s")
            parameters.append(phone)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " GROUP BY clients.id;"

        cur.execute(query, tuple(parameters))
        return cur.fetchall()

if __name__ == "__main__":
    # Создаем подключение к БД
    with psycopg2.connect(database="netology_db", user="postgres", password="123") as conn:
        create_db(conn)

        # Добавляем клиентов
        add_client(conn, "Николай", "Васильевич", "Nike.Vas@internet.ru", phones=["+905053454422", "+905053453311"])
        add_client(conn, "Иван", "Александрович", "Ivan.Alex@internet.ru", phones=["+905053450000"])
        add_client(conn, "Дмитрий", "Дмитриевич", "Dim.Dim@internet.ru")

        # Изменяем данные клиента
        change_client(conn, client_id=1, first_name="Никита", phones=["+79876540000", "+79876541123"])

        # Удаляем телефон клиента
        delete_phone(conn, client_id=1, phone="+79876541123")

        # Находим клиентов по различным параметрам
        print(find_client(conn, first_name="Николай"))
        print(find_client(conn, email="Ivan.Alex@internet.ru"))

        # Удаляем клиента
        delete_client(conn, client_id=3)

    conn.close()